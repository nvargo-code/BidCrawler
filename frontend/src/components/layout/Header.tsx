"use client";

import { RefreshCw } from "lucide-react";
import { formatRelativeTime, formatDateTime } from "@/lib/utils";

interface HeaderProps {
  bidCount?: number;
  onRefresh?: () => void;
  refreshing?: boolean;
  lastUpdated?: string | null;
  filterSlot?: React.ReactNode;
}

// Data older than this is flagged — the daily crawl should land every ~24h.
const STALE_AFTER_MS = 30 * 3_600_000;

export function Header({ bidCount, onRefresh, refreshing, lastUpdated, filterSlot }: HeaderProps) {
  const isStale =
    !!lastUpdated && Date.now() - new Date(lastUpdated).getTime() > STALE_AFTER_MS;

  return (
    <header className="sticky top-0 z-30 bg-background/90 backdrop-blur-md border-b border-border">
      <div className="max-w-2xl mx-auto px-4 h-14 flex items-center justify-between gap-3">
        <div className="flex flex-col justify-center min-w-0">
          <div className="flex items-center gap-2 min-w-0">
            <span className="text-lg font-bold text-foreground tracking-tight">BidFeed</span>
            {bidCount != null && (
              <span className="text-xs text-muted hidden sm:block">
                {bidCount} open bids
              </span>
            )}
          </div>
          {lastUpdated !== undefined && (
            <span
              className={`text-[11px] leading-tight ${isStale ? "text-warning" : "text-muted"}`}
              title={
                lastUpdated
                  ? `Feed last refreshed ${formatDateTime(lastUpdated)}`
                  : "No crawler run recorded yet"
              }
            >
              {isStale && "⚠ "}
              Updated {formatRelativeTime(lastUpdated)}
            </span>
          )}
        </div>

        <div className="flex items-center gap-2">
          {filterSlot}
          {onRefresh && (
            <button
              onClick={onRefresh}
              disabled={refreshing}
              title="Reload bids from the database"
              className="p-2 rounded-lg text-muted hover:text-foreground hover:bg-card transition-colors disabled:opacity-40"
            >
              <RefreshCw size={16} className={refreshing ? "animate-spin" : ""} />
            </button>
          )}
        </div>
      </div>
    </header>
  );
}
