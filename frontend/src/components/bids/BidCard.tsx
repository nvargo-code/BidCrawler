"use client";

import { ExternalLink, MapPin, Clock, Building2, DollarSign } from "lucide-react";
import { cn } from "@/lib/utils";
import { formatDate, daysUntil, formatValue } from "@/lib/utils";
import type { Bid } from "@/types/bid";
import { SOURCE_LABELS, SOURCE_COLORS } from "@/types/bid";

interface BidCardProps {
  bid: Bid;
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

  return <span className={cn("text-xs flex items-center gap-1", urgency)}><Clock size={11} />{label}</span>;
}

export function BidCard({ bid }: BidCardProps) {
  const sourceLabel = SOURCE_LABELS[bid.source_id] ?? bid.source_id;
  const sourceColor = SOURCE_COLORS[bid.source_id] ?? "bg-gray-500/20 text-gray-300";
  const keywords = bid.matched_keywords
    ? bid.matched_keywords.split(",").map(k => k.trim()).filter(Boolean).slice(0, 3)
    : [];

  return (
    <a
      href={bid.bid_url || "#"}
      target="_blank"
      rel="noopener noreferrer"
      className="block group"
    >
      <div className={cn(
        "rounded-xl bg-card border border-border p-4 flex flex-col gap-3",
        "hover:bg-card-hover hover:border-accent/30 transition-all duration-150",
        "active:scale-[0.99]"
      )}>
        {/* Top row: source badge + score + due date */}
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-1.5 flex-wrap">
            <span className={cn("px-2 py-0.5 rounded-full text-xs font-medium", sourceColor)}>
              {sourceLabel}
            </span>
            <ScoreBadge score={bid.match_score} />
          </div>
          <DueDateBadge due={bid.due_date} />
        </div>

        {/* Title */}
        <h3 className="text-sm font-semibold text-foreground line-clamp-2 leading-snug group-hover:text-accent transition-colors">
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

        {/* Keywords + value row */}
        <div className="flex items-center justify-between gap-2">
          <div className="flex flex-wrap gap-1">
            {keywords.map((kw) => (
              <span key={kw} className="px-1.5 py-0.5 rounded text-[10px] bg-background border border-border text-muted">
                {kw}
              </span>
            ))}
          </div>
          <div className="flex items-center gap-2 flex-shrink-0 text-xs text-muted">
            {bid.estimated_value != null && (
              <span className="flex items-center gap-0.5">
                <DollarSign size={11} />
                {formatValue(bid.estimated_value)}
              </span>
            )}
            <ExternalLink size={12} className="opacity-0 group-hover:opacity-60 transition-opacity" />
          </div>
        </div>

        {/* Footer: posted date + bid number */}
        {(bid.posted_date || bid.bid_number) && (
          <div className="pt-2 border-t border-border flex items-center justify-between text-[11px] text-muted">
            {bid.posted_date && <span>Posted {formatDate(bid.posted_date)}</span>}
            {bid.bid_number && <span className="font-mono opacity-60">{bid.bid_number}</span>}
          </div>
        )}
      </div>
    </a>
  );
}
