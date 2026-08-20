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
      const res = await fetch(`${url}/rest/v1/bids?select=id&status=eq.open&limit=1`, {
        headers: { apikey: key, Authorization: `Bearer ${key}` },
      });
      if (res.ok) {
        const data = await res.json();
        bidCount = data.length;
      } else {
        fetchError = `${res.status} ${res.statusText}`;
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
