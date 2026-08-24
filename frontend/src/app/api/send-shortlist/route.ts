import { NextResponse } from "next/server";
import { Resend } from "resend";
import { supabaseAdmin } from "@/lib/supabaseAdmin";
import { buildShortlistEmailHtml, buildShortlistEmailText } from "@/lib/email";
import type { Bid } from "@/types/bid";

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

export async function POST(req: Request) {
  let body: { bids?: Bid[]; recipients?: string[] };
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ ok: false, error: "Invalid JSON body" }, { status: 400 });
  }

  const bids = body.bids ?? [];
  const recipients = (body.recipients ?? []).map(r => r.trim()).filter(Boolean);

  if (bids.length === 0) {
    return NextResponse.json({ ok: false, error: "No bids provided" }, { status: 400 });
  }
  const invalid = recipients.filter(r => !EMAIL_RE.test(r));
  if (recipients.length === 0 || invalid.length > 0) {
    return NextResponse.json(
      { ok: false, error: invalid.length ? `Invalid email(s): ${invalid.join(", ")}` : "No recipients provided" },
      { status: 400 }
    );
  }

  const apiKey = process.env.RESEND_API_KEY;
  if (!apiKey) {
    return NextResponse.json({ ok: false, error: "RESEND_API_KEY is not configured" }, { status: 500 });
  }

  const resend = new Resend(apiKey);
  const subject = `Bid Shortlist — ${bids.length} project${bids.length === 1 ? "" : "s"}`;

  const { error: sendError } = await resend.emails.send({
    from: "Bid Crawler <onboarding@resend.dev>",
    to: recipients,
    subject,
    html: buildShortlistEmailHtml(bids),
    text: buildShortlistEmailText(bids),
  });

  if (sendError) {
    return NextResponse.json({ ok: false, error: sendError.message }, { status: 502 });
  }

  try {
    const admin = supabaseAdmin();
    await admin
      .from("saved_recipients")
      .upsert(
        recipients.map(email => ({ email, last_used_at: new Date().toISOString() })),
        { onConflict: "email" }
      );
  } catch {
    // Email already sent successfully — don't fail the request over recipient bookkeeping.
  }

  return NextResponse.json({ ok: true });
}
