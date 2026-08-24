"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import { supabase } from "@/lib/supabase";
import type { Bid, BidFilters } from "@/types/bid";

const PAGE_SIZE = 20;
const RECENT_WINDOW_HOURS = 48;

export type BidSort = "match" | "recent";

export function useBids(filters: BidFilters, sort: BidSort = "match") {
  const [bids, setBids] = useState<Bid[]>([]);
  const [loading, setLoading] = useState(false);
  const [hasMore, setHasMore] = useState(true);
  const offset = useRef(0);
  const filterKey = JSON.stringify(filters) + "|" + sort;

  const fetchPage = useCallback(
    async (reset: boolean) => {
      if (loading && !reset) return;
      setLoading(true);

      const from = reset ? 0 : offset.current;

      const today = new Date().toISOString().slice(0, 10);

      let q = supabase
        .from("bids")
        .select(
          "id,source_id,bid_number,title,agency,agency_type,due_date,posted_date,estimated_value,location_county,location_city,naics_code,status,bid_url,match_score,matched_keywords,fetched_at"
        )
        .eq("status", "open")
        .gte("match_score", filters.minScore)
        .or(`due_date.is.null,due_date.gte.${today}`);

      if (sort === "recent") {
        const cutoff = new Date(Date.now() - RECENT_WINDOW_HOURS * 3_600_000).toISOString();
        q = q.gte("fetched_at", cutoff).order("fetched_at", { ascending: false });
      } else {
        q = q
          .order("match_score", { ascending: false })
          .order("due_date", { ascending: true, nullsFirst: false });
      }

      q = q.range(from, from + PAGE_SIZE - 1);

      if (filters.counties.length > 0)
        q = q.in("location_county", filters.counties);
      if (filters.sources.length > 0)
        q = q.in("source_id", filters.sources);
      if (filters.agencyTypes.length > 0)
        q = q.in("agency_type", filters.agencyTypes);
      if (filters.search)
        q = q.ilike("title", `%${filters.search}%`);

      const { data, error } = await q;

      if (error) {
        console.error("useBids error:", error);
        setLoading(false);
        return;
      }

      const page = (data ?? []) as Bid[];
      setBids((prev) => (reset ? page : [...prev, ...page]));
      offset.current = from + page.length;
      setHasMore(page.length === PAGE_SIZE);
      setLoading(false);
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [filterKey]
  );

  // Reset + re-fetch whenever filters change
  useEffect(() => {
    offset.current = 0;
    fetchPage(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filterKey]);

  const loadMore = useCallback(() => fetchPage(false), [fetchPage]);

  return { bids, loading, hasMore, loadMore };
}
