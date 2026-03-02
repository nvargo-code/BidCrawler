"""Intercept Bonfire portal network requests to find the public data API."""
from playwright.sync_api import sync_playwright
import time

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )
    page = ctx.new_page()

    api_calls = []

    def handle_request(request):
        if request.resource_type in ("xhr", "fetch", "other"):
            api_calls.append({
                "method": request.method,
                "url": request.url,
                "auth": request.headers.get("authorization", ""),
                "body": request.post_data or "",
            })

    page.on("request", handle_request)

    print("Loading Fort Worth Bonfire portal...")
    try:
        page.goto(
            "https://fortworthtexas.bonfirehub.com/portal/?tab=openOpportunities",
            wait_until="networkidle",
            timeout=45000,
        )
    except Exception as e:
        print(f"Page load warning: {e}")

    time.sleep(5)

    print(f"\n=== {len(api_calls)} XHR/Fetch calls captured ===")
    for call in api_calls:
        print(f"  [{call['method']}] {call['url']}")
        if call["auth"]:
            print(f"    Auth: {call['auth'][:120]}")
        if call["body"]:
            print(f"    Body: {call['body'][:200]}")

    browser.close()
    print("\nDone.")
