"use client";

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { getTimezoneLabel, timezones } from "@/lib/dateTimeUtils";
import { getTimezone, useStore } from "@/lib/store";
import { cn } from "@/lib/utils";
import { Calendar } from "lucide-react";
import { DateTime } from "luxon";
import { CustomDateRangePicker } from "./CustomDateRangePicker";
import { Time } from "./types";

// Convert wellKnown kebab-case to display labels
const wellKnownLabels: Record<string, string> = {
  today: "Today",
  yesterday: "Yesterday",
  "last-3-days": "Last 3 Days",
  "last-7-days": "Last 7 Days",
  "last-14-days": "Last 14 Days",
  "last-30-days": "Last 30 Days",
  "last-60-days": "Last 60 Days",
  "last-30-minutes": "Last 30 Minutes",
  "last-1-hour": "Last 1 Hour",
  "last-6-hours": "Last 6 Hours",
  "last-24-hours": "Last 24 Hours",
  "this-week": "This Week",
  "last-week": "Last Week",
  "this-month": "This Month",
  "last-month": "Last Month",
  "this-year": "This Year",
  "all-time": "All Time",
};

const getLabel = (time: Time) => {
  // Check for wellKnown preset first
  if (time.wellKnown && wellKnownLabels[time.wellKnown]) {
    return wellKnownLabels[time.wellKnown];
  }

  // Use timezone-aware "now" for comparisons
  const tz = getTimezone();
  const now = DateTime.now().setZone(tz);

  if (time.mode === "range") {
    const startFormatted = DateTime.fromISO(time.startDate).toFormat("EEEE, MMM d");
    const endFormatted = DateTime.fromISO(time.endDate).toFormat("EEEE, MMM d");
    return `${startFormatted} - ${endFormatted}`;
  }

  if (time.mode === "past-minutes") {
    if (time.pastMinutesStart >= 60) {
      const hours = Math.floor(time.pastMinutesStart / 60);
      return `Last ${hours} ${hours === 1 ? "Hour" : "Hours"}`;
    }
    return `Last ${time.pastMinutesStart} minutes`;
  }

  if (time.mode === "day") {
    if (time.day === now.toISODate()) {
      return "Today";
    }
    if (time.day === now.minus({ days: 1 }).toISODate()) {
      return "Yesterday";
    }
    return DateTime.fromISO(time.day).toFormat("EEEE, MMM d");
  }
  if (time.mode === "week") {
    if (time.week === now.startOf("week").toISODate()) {
      return "This Week";
    }
    if (time.week === now.minus({ weeks: 1 }).startOf("week").toISODate()) {
      return "Last Week";
    }
    const startDate = DateTime.fromISO(time.week).toFormat("EEEE, MMM d");
    const endDate = DateTime.fromISO(time.week).endOf("week").toFormat("EEEE, MMM d");
    return `${startDate} - ${endDate}`;
  }
  if (time.mode === "month") {
    if (time.month === now.startOf("month").toISODate()) {
      return "This Month";
    }
    if (time.month === now.minus({ months: 1 }).startOf("month").toISODate()) {
      return "Last Month";
    }
    return DateTime.fromISO(time.month).toFormat("MMMM yyyy");
  }
  if (time.mode === "year") {
    if (time.year === now.startOf("year").toISODate()) {
      return "This Year";
    }
    return DateTime.fromISO(time.year).toFormat("yyyy");
  }
  if (time.mode === "all-time") {
    return "All Time";
  }
};

export function DateSelector({
  time,
  setTime,
  pastMinutesEnabled = true,
}: {
  time: Time;
  setTime: (time: Time) => void;
  pastMinutesEnabled?: boolean;
}) {
  const { timezone, setTimezone } = useStore();

  return (
    <DropdownMenu>
      <DropdownMenuTrigger size={"sm"}>
        <Calendar className="hidden sm:block w-4 h-4" />
        {getLabel(time)}
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-52">
        <DropdownMenuItem
          onClick={() => {
            const tz = getTimezone();
            const now = DateTime.now().setZone(tz);
            setTime({
              mode: "day",
              day: now.toISODate()!,
              wellKnown: "today",
            });
          }}
          className={cn(time.wellKnown === "today" && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
        >
          Today
        </DropdownMenuItem>
        <DropdownMenuItem
          onClick={() => {
            const tz = getTimezone();
            const now = DateTime.now().setZone(tz);
            setTime({
              mode: "day",
              day: now.minus({ days: 1 }).toISODate()!,
              wellKnown: "yesterday",
            });
          }}
          className={cn(time.wellKnown === "yesterday" && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
        >
          Yesterday
        </DropdownMenuItem>
        <DropdownMenuItem
          onClick={() => {
            const tz = getTimezone();
            const now = DateTime.now().setZone(tz);
            setTime({
              mode: "range",
              startDate: now.minus({ days: 2 }).toISODate()!,
              endDate: now.toISODate()!,
              wellKnown: "last-3-days",
            });
          }}
          className={cn(time.wellKnown === "last-3-days" && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
        >
          Last 3 Days
        </DropdownMenuItem>
        <DropdownMenuItem
          onClick={() => {
            const tz = getTimezone();
            const now = DateTime.now().setZone(tz);
            setTime({
              mode: "range",
              startDate: now.minus({ days: 6 }).toISODate()!,
              endDate: now.toISODate()!,
              wellKnown: "last-7-days",
            });
          }}
          className={cn(time.wellKnown === "last-7-days" && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
        >
          Last 7 Days
        </DropdownMenuItem>
        <DropdownMenuItem
          onClick={() => {
            const tz = getTimezone();
            const now = DateTime.now().setZone(tz);
            setTime({
              mode: "range",
              startDate: now.minus({ days: 13 }).toISODate()!,
              endDate: now.toISODate()!,
              wellKnown: "last-14-days",
            });
          }}
          className={cn(time.wellKnown === "last-14-days" && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
        >
          Last 14 Days
        </DropdownMenuItem>
        <DropdownMenuItem
          onClick={() => {
            const tz = getTimezone();
            const now = DateTime.now().setZone(tz);
            setTime({
              mode: "range",
              startDate: now.minus({ days: 29 }).toISODate()!,
              endDate: now.toISODate()!,
              wellKnown: "last-30-days",
            });
          }}
          className={cn(time.wellKnown === "last-30-days" && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
        >
          Last 30 Days
        </DropdownMenuItem>
        <DropdownMenuItem
          onClick={() => {
            const tz = getTimezone();
            const now = DateTime.now().setZone(tz);
            setTime({
              mode: "range",
              startDate: now.minus({ days: 59 }).toISODate()!,
              endDate: now.toISODate()!,
              wellKnown: "last-60-days",
            });
          }}
          className={cn(time.wellKnown === "last-60-days" && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
        >
          Last 60 Days
        </DropdownMenuItem>
        {pastMinutesEnabled && (
          <>
            <DropdownMenuSeparator />
            <DropdownMenuItem
              onClick={() =>
                setTime({
                  mode: "past-minutes",
                  pastMinutesStart: 30,
                  pastMinutesEnd: 0,
                  wellKnown: "last-30-minutes",
                })
              }
              className={cn(time.wellKnown === "last-30-minutes" && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
            >
              Last 30 Minutes
            </DropdownMenuItem>
            <DropdownMenuItem
              onClick={() =>
                setTime({
                  mode: "past-minutes",
                  pastMinutesStart: 60,
                  pastMinutesEnd: 0,
                  wellKnown: "last-1-hour",
                })
              }
              className={cn(time.wellKnown === "last-1-hour" && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
            >
              Last 1 Hour
            </DropdownMenuItem>
            <DropdownMenuItem
              onClick={() =>
                setTime({
                  mode: "past-minutes",
                  pastMinutesStart: 360,
                  pastMinutesEnd: 0,
                  wellKnown: "last-6-hours",
                })
              }
              className={cn(time.wellKnown === "last-6-hours" && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
            >
              Last 6 Hours
            </DropdownMenuItem>
            <DropdownMenuItem
              onClick={() =>
                setTime({
                  mode: "past-minutes",
                  pastMinutesStart: 1440,
                  pastMinutesEnd: 0,
                  wellKnown: "last-24-hours",
                })
              }
              className={cn(time.wellKnown === "last-24-hours" && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
            >
              Last 24 Hours
            </DropdownMenuItem>
          </>
        )}
        <DropdownMenuSeparator />
        <DropdownMenuItem
          onClick={() => {
            const tz = getTimezone();
            const now = DateTime.now().setZone(tz);
            setTime({
              mode: "week",
              week: now.startOf("week").toISODate()!,
              wellKnown: "this-week",
            });
          }}
          className={cn(time.wellKnown === "this-week" && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
        >
          This Week
        </DropdownMenuItem>
        <DropdownMenuItem
          onClick={() => {
            const tz = getTimezone();
            const now = DateTime.now().setZone(tz);
            setTime({
              mode: "month",
              month: now.startOf("month").toISODate()!,
              wellKnown: "this-month",
            });
          }}
          className={cn(time.wellKnown === "this-month" && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
        >
          This Month
        </DropdownMenuItem>
        <DropdownMenuItem
          onClick={() => {
            const tz = getTimezone();
            const now = DateTime.now().setZone(tz);
            setTime({
              mode: "year",
              year: now.startOf("year").toISODate()!,
              wellKnown: "this-year",
            });
          }}
          className={cn(time.wellKnown === "this-year" && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
        >
          This Year
        </DropdownMenuItem>
        <DropdownMenuItem
          onClick={() =>
            setTime({
              mode: "all-time",
              wellKnown: "all-time",
            })
          }
          className={cn(time.wellKnown === "all-time" && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
        >
          All Time
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        <CustomDateRangePicker setTime={setTime} time={time} />
        <DropdownMenuSeparator />
        <DropdownMenuSub>
          <DropdownMenuSubTrigger>
            {getTimezoneLabel(timezone)}
          </DropdownMenuSubTrigger>
          <DropdownMenuSubContent>
            {timezones.map(tz => (
              <DropdownMenuItem
                key={tz.value}
                onClick={() => setTimezone(tz.value)}
                className={cn(timezone === tz.value && "bg-neutral-50 dark:bg-neutral-800 font-medium")}
              >
                {tz.label}
              </DropdownMenuItem>
            ))}
          </DropdownMenuSubContent>
        </DropdownMenuSub>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
