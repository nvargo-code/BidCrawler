"use client";

import { useEffect, useState } from "react";
import { supabase } from "@/lib/supabase";

/**
 * Returns the ISO timestamp of the most recent crawler run, read from the
 * `sources` table (each source stamps `last_run_at` on every crawl, and the
 * Python sync pushes it to Supabase). The max across all sources is the last
 * time the feed data was refreshed.
 *
 * Pass a `refreshKey` that changes when the user hits Refresh so the stamp
 * re-fetches alongside the bids. A 60s internal tick keeps the relative label
 * ("3h ago") from going stale while the page sits open.
 */
export function useLastUpdated(refreshKey: number = 0): string | null {
  const [iso, setIso] = useState<string | null>(null);
  const [, setTick] = useState(0);

  useEffect(() => {
    let cancelled = false;
    supabase
      .from("sources")
      .select("last_run_at")
      .not("last_run_at", "is", null)
      .order("last_run_at", { ascending: false })
      .limit(1)
      .maybeSingle()
      .then(({ data, error }) => {
        if (cancelled) return;
        if (error) {
          console.error("useLastUpdated error:", error);
          return;
        }
        if (data?.last_run_at) setIso(data.last_run_at as string);
      });
    return () => {
      cancelled = true;
    };
  }, [refreshKey]);

  useEffect(() => {
    const t = setInterval(() => setTick((n) => n + 1), 60_000);
    return () => clearInterval(t);
  }, []);

  return iso;
}
