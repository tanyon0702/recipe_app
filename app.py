#------------------------------
#依存関係のインポート
#------------------------------
from __future__ import annotations
import json
import os
import secrets
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse
from zoneinfo import ZoneInfo
from flask import Flask, abort, redirect, render_template, request, session, url_for
from recipe import fetch_recipe_by_id, fetch_category_ranking, normalize_recipe, suggest_categories  # type: ignore
from authlib.integrations.flask_client import OAuth
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# 本番向け必須環境変数チェック
# -----------------------------
def _require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"環境変数 {name} が未設定です。")
    return value


def validate_env_for_production() -> None:
    env = (os.getenv("APP_ENV") or "").lower()
    if env != "production":
        return
    _require_env("SECRET_KEY")
    _require_env("GOOGLE_CLIENT_ID")
    _require_env("GOOGLE_CLIENT_SECRET")
    _require_env("RAKUTEN_APP_ID")


# -----------------------------
# ファイルパスとアプリ設定
# -----------------------------
DEFAULT_JSON_PATH = "recipes_stock.json"
JSON_PATH = os.getenv("RECIPES_JSON_PATH", DEFAULT_JSON_PATH)
INDEX_TMPL = "index.html"
ADD_FORM_HTML = "add_form.html"
LOGIN_TMPL = "login.html"
app = Flask(__name__)
validate_env_for_production()
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-change-me")
DB_PATH = os.getenv("APP_DB_PATH", "app.db")
JST = ZoneInfo("Asia/Tokyo")
oauth = OAuth(app)
oauth.register(
    name="google",
    client_id=os.getenv("GOOGLE_CLIENT_ID"),
    client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)
MAX_TOKENS = 1000
DAILY_REFILL = 1000
REFILL_HOUR_JST = 4

# -----------------------------
# Rakuten App ID
# -----------------------------
def get_rakuten_app_id() -> str:
    app_id = (os.getenv("RAKUTEN_APP_ID") or "").strip()
    if not app_id:
        raise RuntimeError("環境変数 RAKUTEN_APP_ID が未設定です。")
    return app_id

# -----------------------------
#hhtp(s)以外のURLを弾く関数
# -----------------------------
def safe_external_url(url: str) -> bool:
    """Allow only http(s) URLs."""
    try:
        u = urlparse(url)
        return u.scheme in ("http", "https") and bool(u.netloc)
    except Exception:
        return False

#-----------------------------
#Sqlite DBを接続する関数
#-----------------------------
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


#-----------------------------
#DBを作成する関数
#-----------------------------
def init_db() -> None:
    with get_db() as db:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                google_sub TEXT UNIQUE,
                email TEXT,
                name TEXT,
                picture TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS recipes (
                recipe_id TEXT PRIMARY KEY,
                title TEXT,
                description TEXT,
                materials TEXT,
                time TEXT,
                cost TEXT,
                rank TEXT,
                pickup INTEGER,
                image TEXT,
                url TEXT,
                publish_day TEXT,
                nickname TEXT,
                shop INTEGER,
                source_category_id TEXT
            );

            CREATE TABLE IF NOT EXISTS user_recipes (
                user_id INTEGER NOT NULL,
                recipe_id TEXT NOT NULL,
                added_at TEXT NOT NULL,
                PRIMARY KEY (user_id, recipe_id),
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id)
            );

            CREATE TABLE IF NOT EXISTS user_tokens (
                user_id INTEGER PRIMARY KEY,
                tokens INTEGER NOT NULL,
                last_refill_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            """
        )

#-----------------------------
#直近の回復時刻（JST 4:00）を確認する関数
#-----------------------------
def most_recent_refill_boundary(now_jst: datetime) -> datetime:
    boundary = now_jst.replace(hour=REFILL_HOUR_JST, minute=0, second=0, microsecond=0)
    if now_jst < boundary:
        boundary -= timedelta(days=1)
    return boundary

#-----------------------------
#ユーザのトークンを確認・回復する関数
#-----------------------------

def ensure_user_tokens(db: sqlite3.Connection, user_id: int) -> int:
    row = db.execute(
        "SELECT tokens, last_refill_at FROM user_tokens WHERE user_id = ?",
        (user_id,),
    ).fetchone()

    now_jst = datetime.now(tz=JST)
    current_boundary = most_recent_refill_boundary(now_jst)
    current_boundary_utc = current_boundary.astimezone(timezone.utc)

    if row is None:
        db.execute(
            "INSERT INTO user_tokens (user_id, tokens, last_refill_at) VALUES (?, ?, ?)",
            (user_id, MAX_TOKENS, current_boundary_utc.isoformat()),
        )
        return MAX_TOKENS

    tokens = int(row["tokens"])
    last_refill_at_raw = row["last_refill_at"]
    try:
        last_refill_at = datetime.fromisoformat(last_refill_at_raw)
    except Exception:
        last_refill_at = current_boundary_utc

    last_refill_jst = last_refill_at.astimezone(JST)
    if last_refill_jst > current_boundary:
        last_refill_jst = current_boundary

#経過日数を計算してその日数分トークンを回復
    delta_days = (current_boundary.date() - last_refill_jst.date()).days
    if delta_days > 0:
        tokens = min(MAX_TOKENS, tokens + delta_days * DAILY_REFILL)
        db.execute(
            "UPDATE user_tokens SET tokens = ?, last_refill_at = ? WHERE user_id = ?",
            (tokens, current_boundary_utc.isoformat(), user_id),
        )

    return tokens

#-----------------------------
#ユーザのトークンを設定する関数
#-----------------------------
def set_user_tokens(db: sqlite3.Connection, user_id: int, tokens: int) -> None:
    db.execute(
        "UPDATE user_tokens SET tokens = ? WHERE user_id = ?",
        (tokens, user_id),
    )

#-----------------------------
#uidが存在していればそのユーザ情報を取得する関数
#-----------------------------
def get_current_user() -> Dict[str, Any] | None:
    uid = session.get("user_id")
    if not uid:
        return None
    with get_db() as db:
        row = db.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()
    return dict(row) if row else None

#-----------------------------
#未ログインならログインへ
#-----------------------------
def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login", next=request.path))
        return fn(*args, **kwargs)

    return wrapper

#-----------------------------
#CSRFトークンを発行・検証
#-----------------------------
def get_csrf_token() -> str:
    token = session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["csrf_token"] = token
    return token


def require_csrf() -> None:
    token = request.form.get("csrf_token") or ""
    session_token = session.get("csrf_token") or ""
    if not token or not secrets.compare_digest(token, session_token):
        abort(400, description="Invalid CSRF token.")

#-----------------------------
#テーブル（スキーマ）を作成
#-----------------------------
init_db()



@app.get("/")
@login_required
def index():
    user = get_current_user()

    #ユーザーのレシピ一覧を取得
    with get_db() as db:
        tokens = ensure_user_tokens(db, user["id"])
        rows = db.execute(
            """
            SELECT r.*
            FROM recipes r
            JOIN user_recipes ur ON ur.recipe_id = r.recipe_id
            WHERE ur.user_id = ?
            """,
            (user["id"],),
        ).fetchall()

    recipes = []

    #材料をJSON配列に変換
    for r in rows:
        item = dict(r)
        mats_raw = item.get("materials")
        if isinstance(mats_raw, str):
            try:
                item["materials"] = json.loads(mats_raw)
            except Exception:
                item["materials"] = []
        recipes.append(item)

    # 検索クエリがあればフィルタリング
    q = (request.args.get("q") or "").strip()
    items = recipes

    #qを小文字化してタイトルと材料に含まれるか確認
    if q:
        q_low = q.lower()

        #
        def hit(r: Dict[str, Any]) -> bool:
            title = (r.get("title") or "")
            mats = r.get("materials") or []
            blob = " ".join([title, *mats])
            return q_low in blob.lower()

        items = [r for r in recipes if hit(r)]

    #データ生成元
    generated_at = "user-db"

    #追加後メッセージ取得
    msg = (request.args.get("msg") or "").strip()

    return render_template(
        INDEX_TMPL,
        recipes=items,
        count=len(recipes),
        generated_at=generated_at,
        q=q,
        msg=msg,
        user=user,
        tokens=tokens,
    )



#-----------------------------
#カテゴリIDからランキングを取得しレシピを追加する関数
#-----------------------------
@app.post("/admin/add_category")
@login_required
def add_category():
    """Fetch category ranking and add recipes into the DB for the current user."""
    require_csrf()
    cid = (request.form.get("category_id") or "").strip()
    if not cid:
        abort(400, description="categoryId is required")

    try:
        app_id = get_rakuten_app_id()
    except Exception as e:
        abort(500, description=str(e))

    #カテゴリランキングを取得
    data = fetch_category_ranking(app_id, cid)
    if not data or "result" not in data:
        abort(502, description="カテゴリランキングの取得に失敗しました")

    items = data.get("result") or []
    new_recipes = [normalize_recipe(item, cid) for item in items if isinstance(item, dict)]

    user = get_current_user()
    if not user:
        return redirect(url_for("login"))

    added = 0
    skipped = 0
    with get_db() as db:
        tokens = ensure_user_tokens(db, user["id"])
        owned = db.execute(
            "SELECT recipe_id FROM user_recipes WHERE user_id = ?",
            (user["id"],),
        ).fetchall()
        owned_ids = {str(r["recipe_id"]) for r in owned}

        for r in new_recipes:
            rid = r.get("recipeId")
            if rid is None:
                continue
            rid_str = str(rid)
            if rid_str in owned_ids:
                skipped += 1
                continue
            if tokens <= 0:
                break

            db.execute(
                """
                INSERT OR REPLACE INTO recipes
                (recipe_id, title, description, materials, time, cost, rank, pickup, image, url, publish_day, nickname, shop, source_category_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rid_str,
                    r.get("title"),
                    r.get("description"),
                    json.dumps(r.get("materials") or [], ensure_ascii=False),
                    r.get("time"),
                    r.get("cost"),
                    str(r.get("rank") or ""),
                    int(r.get("pickup") or 0),
                    r.get("image"),
                    r.get("url"),
                    r.get("publishDay"),
                    r.get("nickname"),
                    int(r.get("shop") or 0),
                    r.get("sourceCategoryId"),
                ),
            )
            db.execute(
                "INSERT OR IGNORE INTO user_recipes (user_id, recipe_id, added_at) VALUES (?, ?, ?)",
                (user["id"], rid_str, datetime.now(tz=timezone.utc).isoformat()),
            )
            tokens -= 1
            added += 1

        set_user_tokens(db, user["id"], tokens)

    return redirect(
        url_for(
            "index",
            msg=f"categoryId={cid} から {added}件 追加しました（既存 {skipped}件）",
        ),
        code=302,
    )


#-----------------------------
#Google OAuthでログインする関数
#-----------------------------
@app.get("/login")
def login():
    next_url = (request.args.get("next") or "").strip()
    if next_url:
        session["next_url"] = next_url
    return render_template(LOGIN_TMPL)

#-----------------------------
#Google OAuth認証処理の関数
#-----------------------------
@app.get("/auth/google")
def auth_google():
    if not oauth.google.client_id or not oauth.google.client_secret:
        abort(500, description="Google OAuth is not configured.")
    nonce = secrets.token_urlsafe(16)
    session["oauth_nonce"] = nonce
    redirect_uri = url_for("auth_google_callback", _external=True)
    return oauth.google.authorize_redirect(redirect_uri, nonce=nonce)

#-----------------------------
#Google OAuth認証コールバックの関数
#-----------------------------
@app.get("/auth/google/callback")
def auth_google_callback():
    token = oauth.google.authorize_access_token()
    nonce = session.pop("oauth_nonce", None)
    userinfo = oauth.google.parse_id_token(token, nonce=nonce)
    if not userinfo:
        abort(400, description="Failed to fetch user info from Google.")

    google_sub = userinfo.get("sub")
    email = userinfo.get("email")
    name = userinfo.get("name") or ""
    picture = userinfo.get("picture") or ""
    now_utc = datetime.now(tz=timezone.utc).isoformat()

    with get_db() as db:
        row = db.execute(
            "SELECT * FROM users WHERE google_sub = ?",
            (google_sub,),
        ).fetchone()
        if row is None:
            db.execute(
                """
                INSERT INTO users (google_sub, email, name, picture, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (google_sub, email, name, picture, now_utc),
            )
            row = db.execute(
                "SELECT * FROM users WHERE google_sub = ?",
                (google_sub,),
            ).fetchone()

        session["user_id"] = row["id"]
        ensure_user_tokens(db, row["id"])

    next_url = session.pop("next_url", None)
    return redirect(next_url or url_for("index"))


#-----------------------------
#ログアウトする関数
#-----------------------------
@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

#-----------------------------
#レシピを開く関数
#-----------------------------
@app.get("/open/<recipe_id>")
@login_required
def open_recipe(recipe_id: str):
    user = get_current_user()
    with get_db() as db:
        row = db.execute(
            """
            SELECT r.url
            FROM recipes r
            JOIN user_recipes ur ON ur.recipe_id = r.recipe_id
            WHERE ur.user_id = ? AND ur.recipe_id = ?
            """,
            (user["id"], str(recipe_id)),
        ).fetchone()

    if not row:
        abort(404, description="recipeId not found")

    url = row["url"]
    if not isinstance(url, str) or not safe_external_url(url):
        abort(400, description="invalid recipe url")

    return redirect(url, code=302)

#-----------------------------
#レシピ追加フォームを表示する関数
#-----------------------------
@app.get("/admin/add")
@login_required
def add_recipe_form():
    user = get_current_user()
    csrf_token = get_csrf_token()

    # Category suggestion (A): natural language -> categoryId candidates
    cq = (request.args.get("cq") or "").strip()
    suggestions: List[Dict[str, Any]] = []
    cat_error = ""
    if cq:
        try:
            suggestions = suggest_categories(get_rakuten_app_id(), cq, limit=10)
        except Exception as e:
            cat_error = str(e)

    with get_db() as db:
        tokens = ensure_user_tokens(db, user["id"])

    return render_template(
        ADD_FORM_HTML,
        error="",
        ok="",
        cq=cq,
        cat_error=cat_error,
        suggestions=suggestions,
        tokens=tokens,
        csrf_token=csrf_token,
    )

#-----------------------------
#レシピを追加する関数
#-----------------------------
@app.post("/admin/add")
@login_required
def add_recipe():
    user = get_current_user()
    if not user:
        return redirect(url_for("login"))

    csrf_token = get_csrf_token()
    require_csrf()

    recipe_id = (request.form.get("recipeId") or "").strip()
    if not recipe_id.isdigit():
        return render_template(
            ADD_FORM_HTML,
            error="recipeId は数字で入力してください。",
            ok="",
            cq="",
            cat_error="",
            suggestions=[],
            tokens=0,
            csrf_token=csrf_token,
        ), 400

    with get_db() as db:
        tokens = ensure_user_tokens(db, user["id"])
        if tokens <= 0:
            return render_template(
                ADD_FORM_HTML,
                error="トークンが不足しています。午前4時（JST）に回復します。",
                ok="",
                cq="",
                cat_error="",
                suggestions=[],
                tokens=tokens,
                csrf_token=csrf_token,
            ), 403

        owned = db.execute(
            "SELECT 1 FROM user_recipes WHERE user_id = ? AND recipe_id = ?",
            (user["id"], recipe_id),
        ).fetchone()

        if owned:
            return render_template(
                ADD_FORM_HTML,
                error="",
                ok="すでにあなたの一覧に入っています。",
                cq="",
                cat_error="",
                suggestions=[],
                tokens=tokens,
                csrf_token=csrf_token,
            )


    r = fetch_recipe_by_id(recipe_id)
    if not r:
        return render_template(
            ADD_FORM_HTML,
            error="取得できませんでした（存在しないID or ページ構造変更の可能性）。",
            ok="",
            tokens=tokens,
            csrf_token=csrf_token,
        ), 404

    with get_db() as db:
        db.execute(
            """
            INSERT OR REPLACE INTO recipes
            (recipe_id, title, description, materials, time, cost, rank, pickup, image, url, publish_day, nickname, shop, source_category_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(r.get("recipeId")),
                r.get("title"),
                r.get("description"),
                json.dumps(r.get("materials") or [], ensure_ascii=False),
                r.get("time"),
                r.get("cost"),
                str(r.get("rank") or ""),
                int(r.get("pickup") or 0),
                r.get("image"),
                r.get("url"),
                r.get("publishDay"),
                r.get("nickname"),
                int(r.get("shop") or 0),
                r.get("sourceCategoryId"),
            ),
        )
        db.execute(
            "INSERT OR IGNORE INTO user_recipes (user_id, recipe_id, added_at) VALUES (?, ?, ?)",
            (user["id"], str(r.get("recipeId")), datetime.now(tz=timezone.utc).isoformat()),
        )
        set_user_tokens(db, user["id"], tokens - 1)

    # Redirect back to index with a small message
    return redirect(url_for("index", msg=f"recipeId={recipe_id} を追加しました"), code=302)


if __name__ == "__main__":
    # ローカルで触る用。必要なら host="0.0.0.0" に。
    init_db()

    debug_flag = os.getenv("FLASK_DEBUG", "").lower() in ("1", "true", "yes", "on")
    app.run(host="0.0.0.0", port=5000, debug=debug_flag)
