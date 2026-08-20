"use client";

import { Home, SlidersHorizontal, CalendarDays } from "lucide-react";
import { cn } from "@/lib/utils";

interface MobileNavProps {
  activeTab: "feed" | "due-soon" | "settings";
  onTabChange: (tab: "feed" | "due-soon" | "settings") => void;
}

export function MobileNav({ activeTab, onTabChange }: MobileNavProps) {
  const items = [
    { id: "feed" as const, icon: Home, label: "Feed" },
    { id: "due-soon" as const, icon: CalendarDays, label: "Due Soon" },
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
              "flex flex-col items-center gap-1 px-4 py-2 rounded-xl transition-colors flex-1",
              activeTab === id ? "text-accent" : "text-muted hover:text-foreground"
            )}
          >
            <Icon size={22} />
            <span className="text-[10px] font-medium">{label}</span>
          </button>
        ))}
      </div>
    </nav>
  );
}
