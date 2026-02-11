import os
import sys
import requests

AUTH_URL = "https://data.judicial.gov.tw/jdg/api/Auth"
JDOC_URL = "https://data.judicial.gov.tw/jdg/api/JDoc"

def die(msg: str, code: int = 1):
    print(msg, file=sys.stderr)
    sys.exit(code)

def main():
    user = os.getenv("JUD_USER")
    password = os.getenv("JUD_PASSWORD")
    if not user or not password:
        die("請先設定環境變數 JUD_USER / JUD_PASSWORD")

    jid = "SJEV,111,重醫簡,3,20251105,3"  # <- 改成你的 JID
    timeout = 20

    # Step 1: Auth 取得 token（token 有效 6 小時）:contentReference[oaicite:1]{index=1}
    r = requests.post(AUTH_URL, json={"user": user, "password": password}, timeout=timeout)
    if r.status_code != 200:
        die(f"Auth HTTP {r.status_code}: {r.text}")
    data = r.json()
    token = data.get("Token") or data.get("token")
    if not token:
        die(f"Auth 回傳沒看到 token: {data}")

    # Step 2: JDoc 用 token + jid 取全文 :contentReference[oaicite:2]{index=2}
    r = requests.post(JDOC_URL, json={"token": token, "j": jid}, timeout=timeout)
    if r.status_code != 200:
        die(f"JDoc HTTP {r.status_code}: {r.text}")

    out = r.json()
    if "error" in out:
        # 規格：查無資料/移除/未公開會回 error message :contentReference[oaicite:3]{index=3}
        die(f'JDoc error: {out["error"]}')

    jfull = out.get("JFULLX", {})
    jtype = jfull.get("JFULLTYPE")
    title = out.get("JTITLE")
    print("OK ✅")
    print("JID   :", out.get("JID"))
    print("TITLE :", title)
    print("TYPE  :", jtype)

    if jtype == "text":
        txt = jfull.get("JFULLCONTENT", "")
        print("\n--- JFULLCONTENT 前 300 字 ---")
        print(txt[:300])
    elif jtype == "file":
        print("\n--- JFULLPDF 下載連結 ---")
        print(jfull.get("JFULLPDF"))
    else:
        print("\n--- 回傳沒有預期的 JFULLTYPE，完整輸出如下 ---")
        print(out)

if __name__ == "__main__":
    main()