"use client";

import { useRef, useState } from "react";
import { MapPin, Clock, Building2, DollarSign, Bookmark } from "lucide-react";
import { cn } from "@/lib/utils";
import { formatDate, daysUntil, formatValue } from "@/lib/utils";
import type { Bid } from "@/types/bid";
import { SOURCE_LABELS, SOURCE_COLORS } from "@/types/bid";

interface BidCardProps {
  bid: Bid;
  shortlisted?: boolean;
  onShortlist?: (bid: Bid) => void;
}

function ScoreBadge({ score }: { score: number }) {
  const color =
    score >= 40 ? "bg-success/20 text-green-300" :
    score >= 25 ? "bg-warning/20 text-yellow-300" :
    "bg-muted/20 text-muted";
  return (
    <span className={cn("inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold tabular-nums", color)}>
      {score}
    </span>
  );
}

function DueDateBadge({ due }: { due: string | null }) {
  const days = daysUntil(due);
  if (days === null) return null;
  if (days < 0) return <span className="text-xs text-muted">Expired</span>;

  const urgency =
    days <= 3 ? "text-error font-semibold" :
    days <= 7 ? "text-warning font-medium" :
    days <= 14 ? "text-yellow-400" :
    "text-muted";

  const label =
    days === 0 ? "Due today!" :
    days === 1 ? "Due tomorrow" :
    `Due in ${days}d`;

  return (
    <span className={cn("text-xs flex items-center gap-1", urgency)}>
      <Clock size={11} />{label}
    </span>
  );
}

export function BidCard({ bid, shortlisted, onShortlist }: BidCardProps) {
  const sourceLabel = SOURCE_LABELS[bid.source_id] ?? bid.source_id;
  const sourceColor = SOURCE_COLORS[bid.source_id] ?? "bg-gray-500/20 text-gray-300";
  const keywords = bid.matched_keywords
    ? bid.matched_keywords.split(",").map(k => k.trim()).filter(Boolean).slice(0, 3)
    : [];

  // Swipe-to-shortlist (mobile)
  const touchStartX = useRef(0);
  const touchStartY = useRef(0);
  const [swipeX, setSwipeX] = useState(0);
  const [saved, setSaved] = useState(false);

  const handleTouchStart = (e: React.TouchEvent) => {
    touchStartX.current = e.touches[0].clientX;
    touchStartY.current = e.touches[0].clientY;
  };

  const handleTouchMove = (e: React.TouchEvent) => {
    const dx = e.touches[0].clientX - touchStartX.current;
    const dy = Math.abs(e.touches[0].clientY - touchStartY.current);
    if (dx > 10 && dx > dy * 1.5) {
      setSwipeX(Math.min(dx, 96));
    }
  };

  const handleTouchEnd = () => {
    if (swipeX > 64) {
      if (!shortlisted) onShortlist?.(bid);
      setSaved(true);
      setTimeout(() => setSaved(false), 900);
    }
    setSwipeX(0);
  };

  const handleClick = () => {
    if (bid.bid_url) window.open(bid.bid_url, "_blank", "noopener,noreferrer");
  };

  const handleBookmark = (e: React.MouseEvent) => {
    e.stopPropagation();
    onShortlist?.(bid);
  };

  return (
    <div className="relative overflow-hidden rounded-xl">
      {/* Swipe reveal layer */}
      <div
        className="absolute inset-0 flex items-center pl-5 rounded-xl"
        style={{
          background: shortlisted ? "rgba(239,68,68,0.15)" : "rgba(34,197,94,0.15)",
          opacity: swipeX / 80,
        }}
      >
        <Bookmark
          size={22}
          className={shortlisted ? "text-red-400" : "text-green-400"}
          fill="currentColor"
        />
      </div>

      {/* Card */}
      <div
        style={{
          transform: `translateX(${swipeX}px)`,
          transition: swipeX === 0 ? "transform 0.25s ease" : "none",
        }}
        onTouchStart={handleTouchStart}
        onTouchMove={handleTouchMove}
        onTouchEnd={handleTouchEnd}
        onClick={handleClick}
        className={cn(
          "rounded-xl bg-card border p-4 flex flex-col gap-3 cursor-pointer",
          shortlisted ? "border-accent/50" : "border-border",
          "hover:bg-card-hover hover:border-accent/30 transition-colors duration-150",
          "active:scale-[0.99]"
        )}
      >
        {/* Top row */}
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1.5 flex-wrap">
            <span className={cn("px-2 py-0.5 rounded-full text-xs font-medium", sourceColor)}>
              {sourceLabel}
            </span>
            <ScoreBadge score={bid.match_score} />
          </div>
          <div className="flex items-center gap-2">
            <DueDateBadge due={bid.due_date} />
            {/* Bookmark button — visible on desktop, hidden on touch devices */}
            {onShortlist && (
              <button
                onClick={handleBookmark}
                title={shortlisted ? "Remove from saved" : "Save bid"}
                className={cn(
                  "p-1 rounded-lg transition-colors hidden sm:flex items-center",
                  shortlisted
                    ? "text-accent"
                    : "text-muted hover:text-accent"
                )}
              >
                <Bookmark size={14} fill={shortlisted ? "currentColor" : "none"} />
              </button>
            )}
          </div>
        </div>

        {/* Title */}
        <h3 className="text-sm font-semibold text-foreground line-clamp-2 leading-snug hover:text-accent transition-colors">
          {bid.title || "Untitled bid"}
        </h3>

        {/* Agency + county */}
        <div className="flex items-start gap-1 text-xs text-muted">
          <Building2 size={12} className="mt-0.5 flex-shrink-0" />
          <span className="line-clamp-1">{bid.agency}</span>
          {bid.location_county && (
            <>
              <span className="mx-1">·</span>
              <MapPin size={11} className="mt-0.5 flex-shrink-0" />
              <span className="flex-shrink-0">{bid.location_county} Co.</span>
            </>
          )}
        </div>

        {/* Keywords + value */}
        <div className="flex items-center justify-between gap-2">
          <div className="flex flex-wrap gap-1">
            {keywords.map((kw) => (
              <span key={kw} className="px-1.5 py-0.5 rounded text-[10px] bg-background border border-border text-muted">
                {kw}
              </span>
            ))}
          </div>
          {bid.estimated_value != null && (
            <span className="flex items-center gap-0.5 text-xs text-muted flex-shrink-0">
              <DollarSign size={11} />
              {formatValue(bid.estimated_value)}
            </span>
          )}
        </div>

        {/* Footer */}
        {(bid.posted_date || bid.bid_number) && (
          <div className="pt-2 border-t border-border flex items-center justify-between text-[11px] text-muted">
            {bid.posted_date && <span>Posted {formatDate(bid.posted_date)}</span>}
            {bid.bid_number && <span className="font-mono opacity-60">{bid.bid_number}</span>}
          </div>
        )}

        {/* Mobile saved flash */}
        {saved && (
          <div className="absolute inset-0 flex items-center justify-center rounded-xl bg-background/80 pointer-events-none">
            <span className="text-sm font-semibold text-green-400">
              {shortlisted ? "Already saved" : "Saved ✓"}
            </span>
          </div>
        )}
      </div>
    </div>
  );
}
