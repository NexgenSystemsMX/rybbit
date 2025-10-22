"use client";

import { useState } from "react";
import { DateTime, DateTimeFormatOptions } from "luxon";
import { CalendarIcon, X } from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Calendar } from "@/components/ui/calendar";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";

export interface DateRange {
  startDate?: DateTime;
  endDate?: DateTime;
}

interface DateRangePickerProps {
  value?: DateRange;
  onChange?: (dateRange: DateRange) => void;
  label?: string;
  disabled?: boolean;
  startDatePlaceholder?: string;
  endDatePlaceholder?: string;
  showClear?: boolean;
  clearButtonText?: string;
  showDescription?: boolean;
  dateFormat?: DateTimeFormatOptions;
  maxDate?: DateTime;
  minDate?: DateTime;
  className?: string;
  layout?: "horizontal" | "vertical";
}

export function SplitDateRangePicker({
  value,
  onChange,
  label = "Date Range",
  disabled = false,
  startDatePlaceholder = "Pick start date",
  endDatePlaceholder = "Pick end date",
  showClear = true,
  clearButtonText = "Clear dates",
  showDescription = true,
  dateFormat = DateTime.DATE_FULL,
  maxDate = DateTime.utc(),
  minDate,
  className,
  layout = "horizontal",
}: DateRangePickerProps) {
  const [internalValue, setInternalValue] = useState<DateRange>({});

  const currentValue = value ?? internalValue;
  const { startDate, endDate } = currentValue;

  const handleDateChange = (newDateRange: DateRange) => {
    if (!value) {
      setInternalValue(newDateRange);
    }
    onChange?.(newDateRange);
  };

  const handleStartDateChange = (date?: Date) => {
    handleDateChange({
      ...currentValue,
      startDate: date ? DateTime.fromJSDate(date, { zone: "utc" }) : undefined,
    });
  };

  const handleEndDateChange = (date?: Date) => {
    handleDateChange({
      ...currentValue,
      endDate: date ? DateTime.fromJSDate(date, { zone: "utc" }) : undefined,
    });
  };

  const handleClear = () => {
    handleDateChange({});
  };

  const hasDateSelected = !!(startDate || endDate);

  const gridClass = layout === "horizontal" ? "grid-cols-1 sm:grid-cols-2" : "grid-cols-1";

  return (
    <div className={cn("space-y-4", className)}>
      {label && (
        <Label className="flex items-center gap-2">
          <CalendarIcon className="h-4 w-4" />
          {label}
        </Label>
      )}

      <div className={cn("grid gap-4", gridClass)}>
        <div className="space-y-2">
          <Label className="text-sm text-muted-foreground">Start Date</Label>
          <Popover>
            <PopoverTrigger asChild>
              <Button
                variant="outline"
                className={cn("w-full justify-start text-left font-normal", !startDate && "text-muted-foreground")}
                disabled={disabled}
              >
                <CalendarIcon className="mr-2 h-4 w-4" />
                {startDate ? startDate.toLocaleString(dateFormat) : startDatePlaceholder}
              </Button>
            </PopoverTrigger>
            <PopoverContent className="w-auto p-0" align="start">
              <Calendar
                mode="single"
                selected={startDate?.toJSDate()}
                onSelect={handleStartDateChange}
                disabled={date => {
                  const d = DateTime.fromJSDate(date, { zone: "utc" }).startOf("day");
                  if (maxDate && d > maxDate.startOf("day")) return true;
                  if (minDate && d < minDate.startOf("day")) return true;
                  if (endDate && d > endDate.startOf("day")) return true;
                  return false;
                }}
                captionLayout="dropdown"
              />
            </PopoverContent>
          </Popover>
        </div>

        <div className="space-y-2">
          <Label className="text-sm text-muted-foreground">End Date</Label>
          <Popover>
            <PopoverTrigger asChild>
              <Button
                variant="outline"
                className={cn("w-full justify-start text-left font-normal", !endDate && "text-muted-foreground")}
                disabled={disabled}
              >
                <CalendarIcon className="mr-2 h-4 w-4" />
                {endDate ? endDate.toLocaleString(dateFormat) : endDatePlaceholder}
              </Button>
            </PopoverTrigger>
            <PopoverContent className="w-auto p-0" align="start">
              <Calendar
                mode="single"
                selected={endDate?.toJSDate()}
                onSelect={handleEndDateChange}
                disabled={date => {
                  const d = DateTime.fromJSDate(date, { zone: "utc" }).startOf("day");
                  if (maxDate && d > maxDate.startOf("day")) return true;
                  if (minDate && d < minDate.startOf("day")) return true;
                  if (startDate && d < startDate.startOf("day")) return true;
                  return false;
                }}
                captionLayout="dropdown"
              />
            </PopoverContent>
          </Popover>
        </div>
      </div>

      {showClear && hasDateSelected && (
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={handleClear} className="shrink-0" disabled={disabled}>
            <X className="h-4 w-4 mr-1" />
            {clearButtonText}
          </Button>
        </div>
      )}

      {showDescription && hasDateSelected && (
        <p className="text-xs text-muted-foreground">
          {startDate && endDate
            ? `Selected range: ${startDate.toLocaleString(dateFormat)} to ${endDate.toLocaleString(dateFormat)}`
            : startDate
              ? `From ${startDate.toLocaleString(dateFormat)} onwards`
              : endDate
                ? `Up to ${endDate.toLocaleString(dateFormat)}`
                : ""}
        </p>
      )}
    </div>
  );
}
