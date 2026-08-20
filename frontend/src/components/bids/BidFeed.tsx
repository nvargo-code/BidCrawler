"use client";

import { useEffect, useRef } from "react";
import { BidCard } from "./BidCard";
import { BidFeedSkeleton } from "@/components/ui/Skeleton";
import { Spinner } from "@/components/ui/Spinner";
import type { Bid } from "@/types/bid";

interface BidFeedProps {
  bids: Bid[];
  loading: boolean;
  hasMore: boolean;
  onLoadMore: () => void;
}

export function BidFeed({ bids, loading, hasMore, onLoadMore }: BidFeedProps) {
  const sentinelRef = useRef<HTMLDivElement>(null);

  // IntersectionObserver — triggers loadMore when the bottom sentinel scrolls into view
  useEffect(() => {
    const sentinel = sentinelRef.current;
    if (!sentinel) return;

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && hasMore && !loading) {
          onLoadMore();
        }
      },
      { rootMargin: "200px" } // start loading 200px before reaching the bottom
    );

    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [hasMore, loading, onLoadMore]);

  if (loading && bids.length === 0) {
    return <BidFeedSkeleton count={8} />;
  }

  if (!loading && bids.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-20 text-center gap-3">
        <p className="text-4xl">🏗️</p>
        <p className="text-foreground font-medium">No bids match your filters</p>
        <p className="text-sm text-muted">Try widening your county or score range</p>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {bids.map((bid) => (
        <BidCard key={bid.id} bid={bid} />
      ))}

      {/* Sentinel div — IntersectionObserver watches this */}
      <div ref={sentinelRef} className="h-1" />

      {/* Loading spinner for subsequent pages */}
      {loading && bids.length > 0 && (
        <div className="flex justify-center py-6">
          <Spinner size="md" />
        </div>
      )}

      {/* End-of-feed message */}
      {!hasMore && bids.length > 0 && (
        <p className="text-center text-xs text-muted py-6">
          {bids.length} bids — refresh to check for new ones
        </p>
      )}
    </div>
  );
}
