"use client";

import { forwardRef, ButtonHTMLAttributes } from "react";
import { cn } from "@/lib/utils";
import { Spinner } from "./Spinner";

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: "primary" | "secondary" | "ghost" | "danger";
  size?: "sm" | "md" | "lg";
  loading?: boolean;
  fullWidth?: boolean;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant = "primary", size = "md", loading = false, fullWidth = false, disabled, children, ...props }, ref) => {
    const variants = {
      primary: "bg-accent text-white hover:bg-accent-hover",
      secondary: "bg-card text-foreground border border-border hover:bg-card-hover",
      ghost: "text-foreground hover:bg-card",
      danger: "bg-error text-white hover:bg-red-600",
    };
    const sizes = {
      sm: "px-3 py-1.5 text-sm min-h-[36px]",
      md: "px-4 py-2 text-base min-h-[44px]",
      lg: "px-6 py-3 text-lg min-h-[52px]",
    };
    return (
      <button
        ref={ref}
        className={cn(
          "inline-flex items-center justify-center font-medium rounded-lg transition-colors focus:outline-none focus:ring-2 focus:ring-accent focus:ring-offset-2 focus:ring-offset-background disabled:opacity-50 disabled:pointer-events-none",
          variants[variant], sizes[size], fullWidth && "w-full", className
        )}
        disabled={disabled || loading}
        {...props}
      >
        {loading ? <><Spinner size="sm" className="mr-2" />Loading…</> : children}
      </button>
    );
  }
);
Button.displayName = "Button";
