import { NextResponse } from "next/server";
import { supabaseAdmin } from "@/lib/supabaseAdmin";

export async function GET() {
  try {
    const { data, error } = await supabaseAdmin()
      .from("saved_recipients")
      .select("email")
      .order("last_used_at", { ascending: false })
      .limit(20);

    if (error) {
      return NextResponse.json({ recipients: [], error: error.message }, { status: 500 });
    }

    return NextResponse.json({ recipients: (data ?? []).map(r => r.email) });
  } catch (e) {
    return NextResponse.json({ recipients: [], error: String(e) }, { status: 500 });
  }
}
