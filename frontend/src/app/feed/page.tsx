"use client";

export const dynamic = "force-dynamic";

import { useState, useCallback } from "react";
import { Header } from "@/components/layout/Header";
import { MobileNav } from "@/components/layout/MobileNav";
import { BidFeed } from "@/components/bids/BidFeed";
import { BidFilters } from "@/components/bids/BidFilters";
import { ShortlistPanel } from "@/components/bids/ShortlistPanel";
import { useBids } from "@/hooks/useBids";
import { useShortlist } from "@/hooks/useShortlist";
import type { BidFilters as BidFiltersType } from "@/types/bid";
import { ESBD_SOURCE_ID } from "@/types/bid";

const DEFAULT_FILTERS: BidFiltersType = {
  counties: [],
  sources: [],
  agencyTypes: [],
  minScore: 10,
  search: "",
};

const DUE_SOON_FILTERS: BidFiltersType = {
  counties: ["Dallas", "Tarrant", "Collin", "Denton", "Rockwall", "Kaufman", "Ellis", "Johnson", "Parker", "Wise", "Hunt", "Grayson"],
  sources: [],
  agencyTypes: [],
  minScore: 10,
  search: "",
};

type Tab = "feed" | "new" | "due-soon" | "esbd" | "saved";

// Tabs whose feed excludes ESBD — ESBD's much larger statewide volume lives
// in its own tab so it doesn't drown out the smaller local DFW platforms.
const LOCAL_ONLY_TABS: Tab[] = ["feed", "new", "due-soon"];

export default function FeedPage() {
  const [tab, setTab] = useState<Tab>("feed");
  const [filters, setFilters] = useState<BidFiltersType>(DEFAULT_FILTERS);
  const [refreshKey, setRefreshKey] = useState(0);

  const shortlist = useShortlist();

  const isEsbdTab = tab === "esbd";
  const baseFilters: BidFiltersType = tab === "due-soon" ? DUE_SOON_FILTERS : filters;
  const activeFilters: BidFiltersType = isEsbdTab
    ? { ...baseFilters, sources: [ESBD_SOURCE_ID] }
    : baseFilters;

  const { bids, loading, hasMore, loadMore } = useBids(
    {
      ...activeFilters,
      search: activeFilters.search + (refreshKey > 0 ? ` ` : ""),
    },
    tab === "new" ? "recent" : "match",
    LOCAL_ONLY_TABS.includes(tab) ? ESBD_SOURCE_ID : undefined
  );

  const handleTabChange = useCallback((t: Tab) => setTab(t), []);

  const handleRefresh = useCallback(() => setRefreshKey(k => k + 1), []);

  const showFeed = tab === "feed" || tab === "new" || tab === "due-soon" || tab === "esbd";

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <Header
        bidCount={showFeed ? bids.length : shortlist.count}
        onRefresh={showFeed ? handleRefresh : undefined}
        refreshing={loading && bids.length === 0}
        filterSlot={
          (tab === "feed" || tab === "new" || tab === "esbd") && (
            <BidFilters
              filters={filters}
              onChange={setFilters}
              totalCount={bids.length}
              hideSourceFilter={isEsbdTab}
            />
          )
        }
      />

      {/* Desktop tab bar */}
      <div className="hidden md:flex gap-1 px-4 py-2 max-w-2xl mx-auto w-full border-b border-border">
        {(["feed", "new", "due-soon", "esbd", "saved"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`relative px-4 py-1.5 rounded-lg text-sm font-medium transition-colors ${
              tab === t ? "bg-card text-foreground" : "text-muted hover:text-foreground"
            }`}
          >
            {t === "feed" ? "All Bids" : t === "new" ? "New" : t === "due-soon" ? "Due Soon" : t === "esbd" ? "ESBD" : "Saved"}
            {t === "saved" && shortlist.count > 0 && (
              <span className="ml-1.5 bg-accent text-white text-[10px] font-bold rounded-full px-1.5 py-0.5">
                {shortlist.count}
              </span>
            )}
          </button>
        ))}
      </div>

      <main className="flex-1 max-w-2xl mx-auto w-full px-4 pt-4 pb-24 md:pb-8">
        {showFeed ? (
          <>
            {(tab === "feed" || tab === "new" || tab === "esbd") && (filters.counties.length > 0 || filters.minScore > 10) && (
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
              shortlistedIds={shortlist.ids}
              onShortlist={shortlist.toggle}
            />
          </>
        ) : (
          <ShortlistPanel
            bids={shortlist.bids}
            onRemove={shortlist.remove}
            onClear={shortlist.clear}
          />
        )}
      </main>

      <MobileNav activeTab={tab} onTabChange={handleTabChange} savedCount={shortlist.count} />
    </div>
  );
}
