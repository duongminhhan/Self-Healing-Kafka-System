import * as React from "react";
import { Slot } from "@radix-ui/react-slot";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";
const buttonVariants = cva("inline-flex items-center justify-center gap-2 rounded-xl text-sm font-medium transition-colors focus-visible:outline-2 focus-visible:outline-emerald-500 disabled:pointer-events-none disabled:opacity-40 cursor-pointer", {
  variants: { variant: { default: "bg-foreground text-background hover:opacity-85", ghost: "hover:bg-muted", outline: "border border-border hover:bg-muted" }, size: { default: "h-10 px-4", icon: "size-10" } },
  defaultVariants: {variant:"default", size:"default"},
});
export function Button({className, variant, size, asChild=false, ...props}: React.ComponentProps<"button"> & VariantProps<typeof buttonVariants> & {asChild?:boolean}) {
  const Comp = asChild ? Slot : "button";
  return <Comp data-slot="button" className={cn(buttonVariants({variant,size,className}))} {...props}/>;
}
