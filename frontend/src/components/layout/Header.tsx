"use client";

import { RefreshCw } from "lucide-react";

interface HeaderProps {
  bidCount?: number;
  onRefresh?: () => void;
  refreshing?: boolean;
  filterSlot?: React.ReactNode;
}

export function Header({ bidCount, onRefresh, refreshing, filterSlot }: HeaderProps) {
  return (
    <header className="sticky top-0 z-30 bg-background/90 backdrop-blur-md border-b border-border">
      <div className="max-w-2xl mx-auto px-4 h-14 flex items-center justify-between gap-3">
        <div className="flex items-center gap-2 min-w-0">
          <span className="text-lg font-bold text-foreground tracking-tight">BidFeed</span>
          {bidCount != null && (
            <span className="text-xs text-muted hidden sm:block">
              {bidCount} open bids
            </span>
          )}
        </div>

        <div className="flex items-center gap-2">
          {filterSlot}
          {onRefresh && (
            <button
              onClick={onRefresh}
              disabled={refreshing}
              title="Refresh bids"
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
