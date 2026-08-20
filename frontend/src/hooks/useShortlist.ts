"use client";
import { useState, useEffect, useCallback } from "react";
import type { Bid } from "@/types/bid";

export function useShortlist() {
  const [map, setMap] = useState<Map<string, Bid>>(new Map());
  const [ready, setReady] = useState(false);

  // Load from localStorage after mount (avoids SSR mismatch)
  useEffect(() => {
    try {
      const raw = localStorage.getItem("bid-shortlist");
      if (raw) setMap(new Map(JSON.parse(raw) as [string, Bid][]));
    } catch {}
    setReady(true);
  }, []);

  // Persist whenever shortlist changes
  useEffect(() => {
    if (!ready) return;
    try {
      localStorage.setItem("bid-shortlist", JSON.stringify([...map.entries()]));
    } catch {}
  }, [map, ready]);

  const toggle = useCallback((bid: Bid) => {
    setMap(prev => {
      const next = new Map(prev);
      if (next.has(bid.id)) next.delete(bid.id);
      else next.set(bid.id, bid);
      return next;
    });
  }, []);

  const remove = useCallback((id: string) => {
    setMap(prev => { const n = new Map(prev); n.delete(id); return n; });
  }, []);

  const clear = useCallback(() => setMap(new Map()), []);

  return {
    bids: [...map.values()],
    count: map.size,
    ids: new Set(map.keys()),
    toggle,
    remove,
    clear,
  };
}
