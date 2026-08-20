import { NextResponse } from "next/server";

export async function GET() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const key = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "";

  const urlOk = url.startsWith("https://") && url.includes(".supabase.co");
  const keyOk = key.length > 20;

  let bidCount: number | null = null;
  let fetchError: string | null = null;

  if (urlOk && keyOk) {
    try {
      const base = url.replace(/\/$/, "");
      const res = await fetch(`${base}/rest/v1/bids?select=id&limit=3`, {
        headers: { apikey: key, Authorization: `Bearer ${key}` },
      });
      const body = await res.text();
      if (res.ok) {
        bidCount = JSON.parse(body).length;
      } else {
        fetchError = `${res.status} — ${body.slice(0, 200)}`;
      }
    } catch (e) {
      fetchError = String(e);
    }
  }

  return NextResponse.json({
    url: url ? url.slice(0, 40) + "…" : "MISSING",
    keyLength: key.length,
    urlOk,
    keyOk,
    bidCount,
    fetchError,
  });
}
