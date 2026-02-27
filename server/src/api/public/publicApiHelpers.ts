import { z } from "zod";

/**
 * Default time range: last 7 days
 */
export function getDefaultTimeParams(): {
  start_date: string;
  end_date: string;
  time_zone: string;
} {
  const now = new Date();
  const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  return {
    start_date: sevenDaysAgo.toISOString().split("T")[0],
    end_date: now.toISOString().split("T")[0],
    time_zone: "UTC",
  };
}

/**
 * Date validation regex for YYYY-MM-DD format
 */
const dateRegex = /^\d{4}-\d{2}-\d{2}$/;

/**
 * Schema for public API query parameters
 */
const publicApiParamsSchema = z.object({
  siteId: z.string().min(1, "siteId is required"),
  from: z
    .string()
    .regex(dateRegex, "Invalid date format. Use YYYY-MM-DD")
    .optional(),
  to: z
    .string()
    .regex(dateRegex, "Invalid date format. Use YYYY-MM-DD")
    .optional(),
  timezone: z
    .string()
    .optional()
    .refine(
      (tz) => {
        if (!tz) return true;
        try {
          Intl.DateTimeFormat(undefined, { timeZone: tz });
          return true;
        } catch {
          return false;
        }
      },
      { message: "Invalid timezone" }
    ),
  bucket: z
    .enum([
      "minute",
      "five_minutes",
      "ten_minutes",
      "fifteen_minutes",
      "hour",
      "day",
      "week",
      "month",
      "year",
    ])
    .optional(),
  limit: z
    .string()
    .optional()
    .transform((val) => (val ? parseInt(val, 10) : undefined))
    .refine((val) => val === undefined || (val > 0 && val <= 1000), {
      message: "limit must be between 1 and 1000",
    }),
  page: z
    .string()
    .optional()
    .transform((val) => (val ? parseInt(val, 10) : undefined))
    .refine((val) => val === undefined || val > 0, {
      message: "page must be greater than 0",
    }),
  eventName: z.string().optional(),
  filters: z.string().optional(),
});

export type PublicApiParams = z.infer<typeof publicApiParamsSchema>;

export interface NormalizedParams {
  siteId: string;
  start_date: string;
  end_date: string;
  time_zone: string;
  bucket?: string;
  limit: number;
  page: number;
  eventName?: string;
  filters?: string;
}

/**
 * Parse and normalize public API query parameters with defaults
 */
export function parsePublicApiParams(query: Record<string, unknown>): {
  success: true;
  data: NormalizedParams;
} | {
  success: false;
  error: string;
} {
  const result = publicApiParamsSchema.safeParse(query);

  if (!result.success) {
    const errors = result.error.errors.map((e) => `${e.path.join(".")}: ${e.message}`);
    return { success: false, error: errors.join("; ") };
  }

  const { siteId, from, to, timezone, bucket, limit, page, eventName, filters } = result.data;
  const defaults = getDefaultTimeParams();

  return {
    success: true,
    data: {
      siteId,
      start_date: from || defaults.start_date,
      end_date: to || defaults.end_date,
      time_zone: timezone || defaults.time_zone,
      bucket,
      limit: limit || 100,
      page: page || 1,
      eventName,
      filters,
    },
  };
}

/**
 * Build response wrapper for consistent API responses
 */
export function buildSuccessResponse<T>(
  data: T,
  meta: {
    siteId: string | number;
    from: string;
    to: string;
    timezone: string;
    page?: number;
    limit?: number;
    totalCount?: number;
  }
) {
  return {
    success: true,
    data,
    meta: {
      siteId: meta.siteId,
      from: meta.from,
      to: meta.to,
      timezone: meta.timezone,
      ...(meta.page !== undefined && { page: meta.page }),
      ...(meta.limit !== undefined && { limit: meta.limit }),
      ...(meta.totalCount !== undefined && { totalCount: meta.totalCount }),
    },
  };
}

/**
 * Build error response
 */
export function buildErrorResponse(error: string, statusCode: number = 500) {
  return {
    success: false,
    error,
    statusCode,
  };
}
