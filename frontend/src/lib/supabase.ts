import { createClient } from "@supabase/supabase-js";
import type { Bid } from "@/types/bid";

// Fallback URLs allow the module to load at build time without env vars.
// Actual data fetching only happens at runtime when the real env vars are set.
const url = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "https://placeholder.supabase.co";
const key = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ?? "placeholder-anon-key";

export const supabase = createClient(url, key);

export type { Bid };
