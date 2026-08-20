"use client";

export const dynamic = "force-dynamic";

import { useState, useCallback } from "react";
import { Header } from "@/components/layout/Header";
import { MobileNav } from "@/components/layout/MobileNav";
import { BidFeed } from "@/components/bids/BidFeed";
import { BidFilters } from "@/components/bids/BidFilters";
import { useBids } from "@/hooks/useBids";
import type { BidFilters as BidFiltersType } from "@/types/bid";

const DEFAULT_FILTERS: BidFiltersType = {
  counties: [],
  sources: [],
  agencyTypes: [],
  minScore: 10,
  search: "",
};

// "Due Soon" preset — bids due within 14 days, DFW only
const DUE_SOON_FILTERS: BidFiltersType = {
  counties: ["Dallas", "Tarrant", "Collin", "Denton", "Rockwall", "Kaufman", "Ellis", "Johnson", "Parker", "Wise", "Hunt", "Grayson"],
  sources: [],
  agencyTypes: [],
  minScore: 10,
  search: "",
};

export default function FeedPage() {
  const [tab, setTab] = useState<"feed" | "due-soon" | "settings">("feed");
  const [filters, setFilters] = useState<BidFiltersType>(DEFAULT_FILTERS);
  const [refreshKey, setRefreshKey] = useState(0);

  // "Due Soon" tab shows DFW bids sorted by proximity to due date
  const activeFilters: BidFiltersType =
    tab === "due-soon" ? DUE_SOON_FILTERS : filters;

  const { bids, loading, hasMore, loadMore } = useBids({
    ...activeFilters,
    // force re-fetch on refresh
    search: activeFilters.search + (refreshKey > 0 ? ` ` : ""),
  });

  const handleTabChange = useCallback((t: "feed" | "due-soon" | "settings") => {
    if (t === "settings") {
      // "settings" tab just opens the filter modal rather than switching view
      setTab("feed");
    } else {
      setTab(t);
    }
  }, []);

  const handleRefresh = useCallback(() => {
    setRefreshKey((k) => k + 1);
  }, []);

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <Header
        bidCount={bids.length}
        onRefresh={handleRefresh}
        refreshing={loading && bids.length === 0}
        filterSlot={
          tab === "feed" && (
            <BidFilters
              filters={filters}
              onChange={setFilters}
              totalCount={bids.length}
            />
          )
        }
      />

      {/* Tab bar (desktop) */}
      <div className="hidden md:flex gap-1 px-4 py-2 max-w-2xl mx-auto w-full border-b border-border">
        {(["feed", "due-soon"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-colors ${
              tab === t ? "bg-card text-foreground" : "text-muted hover:text-foreground"
            }`}
          >
            {t === "feed" ? "All Bids" : "Due Soon"}
          </button>
        ))}
      </div>

      <main className="flex-1 max-w-2xl mx-auto w-full px-4 pt-4 pb-24 md:pb-8">
        {tab === "feed" && (
          <div className="mb-4 flex items-center gap-2 overflow-x-auto scrollbar-hide">
            {filters.counties.length > 0 && (
              <span className="text-xs text-muted flex-shrink-0">
                {filters.counties.join(", ")} ·
              </span>
            )}
            {filters.minScore > 10 && (
              <span className="text-xs text-muted flex-shrink-0">Score ≥{filters.minScore}</span>
            )}
          </div>
        )}

        <BidFeed
          bids={bids}
          loading={loading}
          hasMore={hasMore}
          onLoadMore={loadMore}
        />
      </main>

      <MobileNav activeTab={tab} onTabChange={handleTabChange} />
    </div>
  );
}
