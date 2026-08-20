"use client";

import { useState } from "react";
import { Filter, X } from "lucide-react";
import { Modal, ModalBody, ModalFooter } from "@/components/ui/Modal";
import { Button } from "@/components/ui/Button";
import { Tag } from "@/components/ui/Tag";
import type { BidFilters } from "@/types/bid";
import { DFW_COUNTIES, SOURCE_LABELS } from "@/types/bid";

const AGENCY_TYPES = ["city", "county", "school", "state", "university", "authority"];

const ALL_SOURCES = Object.entries(SOURCE_LABELS).map(([id, label]) => ({ id, label }));

interface BidFiltersProps {
  filters: BidFilters;
  onChange: (f: BidFilters) => void;
  totalCount?: number;
}

export function BidFilters({ filters, onChange, totalCount }: BidFiltersProps) {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState<BidFilters>(filters);

  const activeCount =
    filters.counties.length +
    filters.sources.length +
    filters.agencyTypes.length +
    (filters.minScore > 10 ? 1 : 0) +
    (filters.search ? 1 : 0);

  const handleApply = () => {
    onChange(draft);
    setOpen(false);
  };

  const handleClear = () => {
    const blank: BidFilters = { counties: [], sources: [], agencyTypes: [], minScore: 10, search: "" };
    setDraft(blank);
    onChange(blank);
    setOpen(false);
  };

  const toggle = <T extends string>(arr: T[], val: T): T[] =>
    arr.includes(val) ? arr.filter((x) => x !== val) : [...arr, val];

  return (
    <>
      {/* Filter button — shown in header */}
      <button
        onClick={() => { setDraft(filters); setOpen(true); }}
        className="relative flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-card border border-border text-sm text-foreground hover:bg-card-hover transition-colors"
      >
        <Filter size={14} />
        Filters
        {activeCount > 0 && (
          <span className="absolute -top-1.5 -right-1.5 w-5 h-5 rounded-full bg-accent text-white text-[10px] font-bold flex items-center justify-center">
            {activeCount}
          </span>
        )}
      </button>

      <Modal isOpen={open} onClose={() => setOpen(false)} title="Filter Bids">
        <ModalBody className="max-h-[70vh] overflow-y-auto space-y-6">

          {/* Search */}
          <div>
            <label className="block text-xs font-semibold text-muted uppercase tracking-wider mb-2">
              Keyword Search
            </label>
            <input
              type="text"
              placeholder="Search titles…"
              value={draft.search}
              onChange={(e) => setDraft({ ...draft, search: e.target.value })}
              className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm text-foreground placeholder:text-muted focus:outline-none focus:ring-1 focus:ring-accent"
            />
          </div>

          {/* Min Score */}
          <div>
            <label className="block text-xs font-semibold text-muted uppercase tracking-wider mb-2">
              Min Match Score: <span className="text-foreground">{draft.minScore}</span>
            </label>
            <input
              type="range"
              min={0}
              max={80}
              step={5}
              value={draft.minScore}
              onChange={(e) => setDraft({ ...draft, minScore: Number(e.target.value) })}
              className="w-full accent-accent"
            />
            <div className="flex justify-between text-[10px] text-muted mt-1">
              <span>All</span><span>Precise</span>
            </div>
          </div>

          {/* Counties */}
          <div>
            <label className="block text-xs font-semibold text-muted uppercase tracking-wider mb-2">
              County
            </label>
            <div className="flex flex-wrap gap-1.5">
              {DFW_COUNTIES.map((c) => (
                <Tag
                  key={c}
                  size="sm"
                  selected={draft.counties.includes(c)}
                  onClick={() => setDraft({ ...draft, counties: toggle(draft.counties, c) })}
                >
                  {c}
                </Tag>
              ))}
            </div>
          </div>

          {/* Agency Type */}
          <div>
            <label className="block text-xs font-semibold text-muted uppercase tracking-wider mb-2">
              Agency Type
            </label>
            <div className="flex flex-wrap gap-1.5">
              {AGENCY_TYPES.map((t) => (
                <Tag
                  key={t}
                  size="sm"
                  selected={draft.agencyTypes.includes(t)}
                  onClick={() => setDraft({ ...draft, agencyTypes: toggle(draft.agencyTypes, t) })}
                >
                  {t}
                </Tag>
              ))}
            </div>
          </div>

          {/* Sources */}
          <div>
            <label className="block text-xs font-semibold text-muted uppercase tracking-wider mb-2">
              Source
            </label>
            <div className="flex flex-wrap gap-1.5">
              {ALL_SOURCES.map(({ id, label }) => (
                <Tag
                  key={id}
                  size="sm"
                  selected={draft.sources.includes(id)}
                  onClick={() => setDraft({ ...draft, sources: toggle(draft.sources, id) })}
                >
                  {label}
                </Tag>
              ))}
            </div>
          </div>

        </ModalBody>

        <ModalFooter className="justify-between">
          <Button variant="ghost" size="sm" onClick={handleClear}>
            <X size={14} className="mr-1" /> Clear all
          </Button>
          <Button size="sm" onClick={handleApply}>
            Show {totalCount != null ? `${totalCount} ` : ""}results
          </Button>
        </ModalFooter>
      </Modal>
    </>
  );
}
