"use client";

import { Home, SlidersHorizontal, CalendarDays, Bookmark } from "lucide-react";
import { cn } from "@/lib/utils";

interface MobileNavProps {
  activeTab: "feed" | "due-soon" | "saved" | "settings";
  onTabChange: (tab: "feed" | "due-soon" | "saved" | "settings") => void;
  savedCount?: number;
}

export function MobileNav({ activeTab, onTabChange, savedCount }: MobileNavProps) {
  const items = [
    { id: "feed" as const, icon: Home, label: "Feed" },
    { id: "due-soon" as const, icon: CalendarDays, label: "Due Soon" },
    { id: "saved" as const, icon: Bookmark, label: "Saved" },
    { id: "settings" as const, icon: SlidersHorizontal, label: "Filters" },
  ];

  return (
    <nav className="fixed bottom-0 left-0 right-0 z-40 bg-card/95 backdrop-blur-md border-t border-border md:hidden">
      <div className="flex justify-around items-center h-16 px-2 pb-safe">
        {items.map(({ id, icon: Icon, label }) => (
          <button
            key={id}
            onClick={() => onTabChange(id)}
            className={cn(
              "flex flex-col items-center gap-1 px-4 py-2 rounded-xl transition-colors flex-1 relative",
              activeTab === id ? "text-accent" : "text-muted hover:text-foreground"
            )}
          >
            <Icon size={22} />
            {id === "saved" && savedCount != null && savedCount > 0 && (
              <span className="absolute top-1 right-3 bg-accent text-white text-[9px] font-bold rounded-full min-w-[16px] h-4 flex items-center justify-center px-1">
                {savedCount}
              </span>
            )}
            <span className="text-[10px] font-medium">{label}</span>
          </button>
        ))}
      </div>
    </nav>
  );
}
