import { FastifyReply, FastifyRequest } from "fastify";
import { clickhouse } from "../../db/clickhouse/clickhouse.js";
import { getFilterStatement } from "../analytics/utils/getFilterStatement.js";
import {
  getTimeStatement,
  processResults,
  TimeBucketToFn,
  bucketIntervalMap,
  enrichWithTraits,
} from "../analytics/utils/utils.js";
import {
  parsePublicApiParams,
  buildSuccessResponse,
  buildErrorResponse,
  NormalizedParams,
} from "./publicApiHelpers.js";
import { getResolvedSiteId } from "./publicApiMiddleware.js";
import SqlString from "sqlstring";

type PublicApiQuerystring = {
  siteId?: string;
  from?: string;
  to?: string;
  timezone?: string;
  bucket?: string;
  limit?: string;
  page?: string;
  eventName?: string;
  filters?: string;
};

/**
 * GET /api/public/analytics/overview
 * Returns aggregate metrics for the specified site and time range
 */
export async function getPublicOverview(
  request: FastifyRequest<{ Querystring: PublicApiQuerystring }>,
  reply: FastifyReply
) {
  const parsed = parsePublicApiParams(request.query as Record<string, unknown>);
  if (!parsed.success) {
    return reply.status(400).send(buildErrorResponse(parsed.error, 400));
  }

  const params = parsed.data;
  const siteId = getResolvedSiteId(request);

  const timeStatement = getTimeStatement({
    start_date: params.start_date,
    end_date: params.end_date,
    time_zone: params.time_zone,
  });

  const filterStatement = getFilterStatement(params.filters || "", siteId, timeStatement);

  const query = `
    WITH
    AllSessionPageviews AS (
        SELECT
            session_id,
            COUNT(CASE WHEN type = 'pageview' THEN 1 END) AS total_pageviews_in_session
        FROM events
        WHERE
            site_id = {siteId:Int32}
            ${timeStatement}
        GROUP BY session_id
    ),
    FilteredSessions AS (
        SELECT
            session_id,
            MIN(timestamp) AS start_time,
            MAX(timestamp) AS end_time
        FROM events
        WHERE
            site_id = {siteId:Int32}
            ${filterStatement}
            ${timeStatement}
        GROUP BY session_id
    ),
    SessionsWithPageviews AS (
        SELECT
            fs.session_id,
            fs.start_time,
            fs.end_time,
            asp.total_pageviews_in_session
        FROM FilteredSessions fs
        LEFT JOIN AllSessionPageviews asp ON fs.session_id = asp.session_id
    )
    SELECT
        session_stats.sessions,
        session_stats.pages_per_session,
        session_stats.bounce_rate * 100 AS bounce_rate,
        session_stats.session_duration,
        page_stats.pageviews,
        page_stats.users
    FROM
    (
        SELECT
            COUNT() AS sessions,
            AVG(total_pageviews_in_session) AS pages_per_session,
            sumIf(1, total_pageviews_in_session = 1) / COUNT() AS bounce_rate,
            AVG(end_time - start_time) AS session_duration
        FROM SessionsWithPageviews
    ) AS session_stats
    CROSS JOIN
    (
        SELECT
            COUNT(CASE WHEN type = 'pageview' THEN 1 END) AS pageviews,
            COUNT(DISTINCT user_id) AS users
        FROM events
        WHERE
            site_id = {siteId:Int32}
            ${filterStatement}
            ${timeStatement}
    ) AS page_stats`;

  try {
    const result = await clickhouse.query({
      query,
      format: "JSONEachRow",
      query_params: { siteId },
    });

    const data = await processResults<{
      sessions: number;
      pageviews: number;
      users: number;
      pages_per_session: number;
      bounce_rate: number;
      session_duration: number;
    }>(result);

    return reply.send(
      buildSuccessResponse(data[0] || {}, {
        siteId,
        from: params.start_date,
        to: params.end_date,
        timezone: params.time_zone,
      })
    );
  } catch (error) {
    console.error("Error fetching public overview:", error);
    return reply.status(500).send(buildErrorResponse("Failed to fetch overview data", 500));
  }
}

/**
 * GET /api/public/analytics/overview-bucketed
 * Returns time-series metrics bucketed by specified interval
 */
export async function getPublicOverviewBucketed(
  request: FastifyRequest<{ Querystring: PublicApiQuerystring }>,
  reply: FastifyReply
) {
  const parsed = parsePublicApiParams(request.query as Record<string, unknown>);
  if (!parsed.success) {
    return reply.status(400).send(buildErrorResponse(parsed.error, 400));
  }

  const params = parsed.data;
  const siteId = getResolvedSiteId(request);
  const bucket = (params.bucket || "day") as keyof typeof TimeBucketToFn;

  if (!TimeBucketToFn[bucket]) {
    return reply.status(400).send(buildErrorResponse(`Invalid bucket: ${bucket}`, 400));
  }

  const timeStatement = getTimeStatement({
    start_date: params.start_date,
    end_date: params.end_date,
    time_zone: params.time_zone,
  });

  const filterStatement = getFilterStatement(params.filters || "", siteId, timeStatement);

  const query = `
WITH
AllSessionPageviews AS (
    SELECT
        session_id,
        countIf(type = 'pageview') AS total_pageviews_in_session
    FROM events
    WHERE
        site_id = {siteId:Int32}
        ${timeStatement}
    GROUP BY session_id
),
FilteredSessions AS (
    SELECT
        session_id,
        MIN(timestamp) AS start_time,
        MAX(timestamp) AS end_time
    FROM events
    WHERE
        site_id = {siteId:Int32}
        ${filterStatement}
        ${timeStatement}
    GROUP BY session_id
),
SessionsWithPageviews AS (
    SELECT
        fs.session_id,
        fs.start_time,
        fs.end_time,
        asp.total_pageviews_in_session
    FROM FilteredSessions fs
    LEFT JOIN AllSessionPageviews asp ON fs.session_id = asp.session_id
)
SELECT
    session_stats.time AS time,
    session_stats.sessions,
    session_stats.pages_per_session,
    session_stats.bounce_rate * 100 AS bounce_rate,
    session_stats.session_duration,
    page_stats.pageviews,
    page_stats.users
FROM
(
    SELECT
        toDateTime(${TimeBucketToFn[bucket]}(toTimeZone(start_time, ${SqlString.escape(params.time_zone)}))) AS time,
        COUNT() AS sessions,
        AVG(total_pageviews_in_session) AS pages_per_session,
        sumIf(1, total_pageviews_in_session = 1) / COUNT() AS bounce_rate,
        AVG(end_time - start_time) AS session_duration
    FROM SessionsWithPageviews
    GROUP BY time ORDER BY time
    WITH FILL FROM toTimeZone(
      toDateTime(${TimeBucketToFn[bucket]}(toDateTime(${SqlString.escape(params.start_date)}, ${SqlString.escape(params.time_zone)}))),
      'UTC'
    )
    TO toTimeZone(
      toDateTime(${TimeBucketToFn[bucket]}(toDateTime(${SqlString.escape(params.end_date)}, ${SqlString.escape(params.time_zone)}))) + INTERVAL 1 DAY,
      'UTC'
    ) STEP INTERVAL ${bucketIntervalMap[bucket]}
) AS session_stats
FULL JOIN
(
    SELECT
        toDateTime(${TimeBucketToFn[bucket]}(toTimeZone(timestamp, ${SqlString.escape(params.time_zone)}))) AS time,
        countIf(type = 'pageview') AS pageviews,
        COUNT(DISTINCT user_id) AS users
    FROM events
    WHERE
        site_id = {siteId:Int32}
        ${filterStatement}
        ${timeStatement}
    GROUP BY time ORDER BY time
    WITH FILL FROM toTimeZone(
      toDateTime(${TimeBucketToFn[bucket]}(toDateTime(${SqlString.escape(params.start_date)}, ${SqlString.escape(params.time_zone)}))),
      'UTC'
    )
    TO toTimeZone(
      toDateTime(${TimeBucketToFn[bucket]}(toDateTime(${SqlString.escape(params.end_date)}, ${SqlString.escape(params.time_zone)}))) + INTERVAL 1 DAY,
      'UTC'
    ) STEP INTERVAL ${bucketIntervalMap[bucket]}
) AS page_stats
USING time
ORDER BY time`;

  try {
    const result = await clickhouse.query({
      query,
      format: "JSONEachRow",
      query_params: { siteId },
    });

    const data = await processResults<{
      time: string;
      sessions: number;
      pageviews: number;
      users: number;
      pages_per_session: number;
      bounce_rate: number;
      session_duration: number;
    }>(result);

    return reply.send(
      buildSuccessResponse(data, {
        siteId,
        from: params.start_date,
        to: params.end_date,
        timezone: params.time_zone,
      })
    );
  } catch (error) {
    console.error("Error fetching public overview-bucketed:", error);
    return reply.status(500).send(buildErrorResponse("Failed to fetch time-series data", 500));
  }
}

/**
 * GET /api/public/analytics/pageviews
 * Returns pageview events with pagination
 */
export async function getPublicPageviews(
  request: FastifyRequest<{ Querystring: PublicApiQuerystring }>,
  reply: FastifyReply
) {
  const parsed = parsePublicApiParams(request.query as Record<string, unknown>);
  if (!parsed.success) {
    return reply.status(400).send(buildErrorResponse(parsed.error, 400));
  }

  const params = parsed.data;
  const siteId = getResolvedSiteId(request);

  const timeStatement = getTimeStatement({
    start_date: params.start_date,
    end_date: params.end_date,
    time_zone: params.time_zone,
  });

  const filterStatement = getFilterStatement(params.filters || "", siteId, timeStatement);
  const offset = (params.page - 1) * params.limit;

  const query = `
    SELECT
      timestamp,
      session_id,
      user_id,
      identified_user_id,
      pathname,
      page_title,
      hostname,
      referrer,
      browser,
      operating_system,
      device_type,
      country,
      region,
      city
    FROM events
    WHERE
      site_id = {siteId:Int32}
      AND type = 'pageview'
      ${timeStatement}
      ${filterStatement}
    ORDER BY timestamp DESC
    LIMIT {limit:Int32} OFFSET {offset:Int32}
  `;

  const countQuery = `
    SELECT COUNT() AS total_count
    FROM events
    WHERE
      site_id = {siteId:Int32}
      AND type = 'pageview'
      ${timeStatement}
      ${filterStatement}
  `;

  try {
    const [result, countResult] = await Promise.all([
      clickhouse.query({
        query,
        format: "JSONEachRow",
        query_params: { siteId, limit: params.limit, offset },
      }),
      clickhouse.query({
        query: countQuery,
        format: "JSONEachRow",
        query_params: { siteId },
      }),
    ]);

    const data = await processResults<Record<string, unknown>>(result);
    const countData = await processResults<{ total_count: number }>(countResult);
    const totalCount = countData[0]?.total_count || 0;

    return reply.send(
      buildSuccessResponse(data, {
        siteId,
        from: params.start_date,
        to: params.end_date,
        timezone: params.time_zone,
        page: params.page,
        limit: params.limit,
        totalCount,
      })
    );
  } catch (error) {
    console.error("Error fetching public pageviews:", error);
    return reply.status(500).send(buildErrorResponse("Failed to fetch pageviews", 500));
  }
}

/**
 * GET /api/public/analytics/events
 * Returns custom events with optional filtering by event name
 */
export async function getPublicEvents(
  request: FastifyRequest<{ Querystring: PublicApiQuerystring }>,
  reply: FastifyReply
) {
  const parsed = parsePublicApiParams(request.query as Record<string, unknown>);
  if (!parsed.success) {
    return reply.status(400).send(buildErrorResponse(parsed.error, 400));
  }

  const params = parsed.data;
  const siteId = getResolvedSiteId(request);

  const timeStatement = getTimeStatement({
    start_date: params.start_date,
    end_date: params.end_date,
    time_zone: params.time_zone,
  });

  const filterStatement = getFilterStatement(params.filters || "", siteId, timeStatement);
  const offset = (params.page - 1) * params.limit;

  const eventNameFilter = params.eventName
    ? `AND event_name = ${SqlString.escape(params.eventName)}`
    : "";

  const query = `
    SELECT
      timestamp,
      event_name,
      toString(props) as properties,
      session_id,
      user_id,
      identified_user_id,
      pathname,
      hostname,
      browser,
      operating_system,
      device_type,
      country,
      type
    FROM events
    WHERE
      site_id = {siteId:Int32}
      AND type IN ('custom_event', 'pageview', 'outbound', 'button_click', 'copy', 'form_submit', 'input_change')
      ${eventNameFilter}
      ${timeStatement}
      ${filterStatement}
    ORDER BY timestamp DESC
    LIMIT {limit:Int32} OFFSET {offset:Int32}
  `;

  const countQuery = `
    SELECT COUNT() AS total_count
    FROM events
    WHERE
      site_id = {siteId:Int32}
      AND type IN ('custom_event', 'pageview', 'outbound', 'button_click', 'copy', 'form_submit', 'input_change')
      ${eventNameFilter}
      ${timeStatement}
      ${filterStatement}
  `;

  try {
    const [result, countResult] = await Promise.all([
      clickhouse.query({
        query,
        format: "JSONEachRow",
        query_params: { siteId, limit: params.limit, offset },
      }),
      clickhouse.query({
        query: countQuery,
        format: "JSONEachRow",
        query_params: { siteId },
      }),
    ]);

    const data = await processResults<Record<string, unknown>>(result);
    const countData = await processResults<{ total_count: number }>(countResult);
    const totalCount = countData[0]?.total_count || 0;

    return reply.send(
      buildSuccessResponse(data, {
        siteId,
        from: params.start_date,
        to: params.end_date,
        timezone: params.time_zone,
        page: params.page,
        limit: params.limit,
        totalCount,
      })
    );
  } catch (error) {
    console.error("Error fetching public events:", error);
    return reply.status(500).send(buildErrorResponse("Failed to fetch events", 500));
  }
}

/**
 * GET /api/public/analytics/sessions
 * Returns session list with aggregated data
 */
export async function getPublicSessions(
  request: FastifyRequest<{ Querystring: PublicApiQuerystring }>,
  reply: FastifyReply
) {
  const parsed = parsePublicApiParams(request.query as Record<string, unknown>);
  if (!parsed.success) {
    return reply.status(400).send(buildErrorResponse(parsed.error, 400));
  }

  const params = parsed.data;
  const siteId = getResolvedSiteId(request);

  const timeStatement = getTimeStatement({
    start_date: params.start_date,
    end_date: params.end_date,
    time_zone: params.time_zone,
  });

  const filterStatement = getFilterStatement(params.filters || "", siteId, timeStatement, {
    sessionLevelParams: ["event_name", "pathname", "page_title"],
  });

  const offset = (params.page - 1) * params.limit;

  const query = `
  WITH AggregatedSessions AS (
      SELECT
          session_id,
          argMax(user_id, timestamp) AS user_id,
          argMax(identified_user_id, timestamp) AS identified_user_id,
          argMax(country, timestamp) AS country,
          argMax(region, timestamp) AS region,
          argMax(city, timestamp) AS city,
          argMax(language, timestamp) AS language,
          argMax(device_type, timestamp) AS device_type,
          argMax(browser, timestamp) AS browser,
          argMax(operating_system, timestamp) AS operating_system,
          argMin(referrer, timestamp) AS referrer,
          argMin(channel, timestamp) AS channel,
          argMin(hostname, timestamp) AS hostname,
          MAX(timestamp) AS session_end,
          MIN(timestamp) AS session_start,
          dateDiff('second', MIN(timestamp), MAX(timestamp)) AS session_duration,
          argMinIf(pathname, timestamp, type = 'pageview') AS entry_page,
          argMaxIf(pathname, timestamp, type = 'pageview') AS exit_page,
          countIf(type = 'pageview') AS pageviews,
          countIf(type = 'custom_event') AS events
      FROM events
      WHERE
          site_id = {siteId:Int32}
          ${timeStatement}
      GROUP BY session_id
      ORDER BY session_end DESC
  )
  SELECT *
  FROM AggregatedSessions
  WHERE 1 = 1 ${filterStatement}
  LIMIT {limit:Int32} OFFSET {offset:Int32}
  `;

  try {
    const result = await clickhouse.query({
      query,
      format: "JSONEachRow",
      query_params: { siteId, limit: params.limit, offset },
    });

    const data = await processResults<{
      session_id: string;
      user_id: string;
      identified_user_id: string;
      country: string;
      region: string;
      city: string;
      language: string;
      device_type: string;
      browser: string;
      operating_system: string;
      referrer: string;
      channel: string;
      hostname: string;
      session_end: string;
      session_start: string;
      session_duration: number;
      entry_page: string;
      exit_page: string;
      pageviews: number;
      events: number;
    }>(result);

    // Enrich with traits from Postgres
    const dataWithTraits = await enrichWithTraits(data, siteId);

    return reply.send(
      buildSuccessResponse(dataWithTraits, {
        siteId,
        from: params.start_date,
        to: params.end_date,
        timezone: params.time_zone,
        page: params.page,
        limit: params.limit,
      })
    );
  } catch (error) {
    console.error("Error fetching public sessions:", error);
    return reply.status(500).send(buildErrorResponse("Failed to fetch sessions", 500));
  }
}

/**
 * GET /api/public/analytics/users
 * Returns user list with aggregated data
 */
export async function getPublicUsers(
  request: FastifyRequest<{ Querystring: PublicApiQuerystring }>,
  reply: FastifyReply
) {
  const parsed = parsePublicApiParams(request.query as Record<string, unknown>);
  if (!parsed.success) {
    return reply.status(400).send(buildErrorResponse(parsed.error, 400));
  }

  const params = parsed.data;
  const siteId = getResolvedSiteId(request);

  const timeStatement = getTimeStatement({
    start_date: params.start_date,
    end_date: params.end_date,
    time_zone: params.time_zone,
  });

  const filterStatement = getFilterStatement(params.filters || "", siteId, timeStatement);
  const offset = (params.page - 1) * params.limit;

  const query = `
WITH AggregatedUsers AS (
    SELECT
        COALESCE(NULLIF(events.identified_user_id, ''), events.user_id) AS effective_user_id,
        argMax(user_id, timestamp) AS user_id,
        argMax(identified_user_id, timestamp) AS identified_user_id,
        argMax(country, timestamp) AS country,
        argMax(region, timestamp) AS region,
        argMax(city, timestamp) AS city,
        argMax(language, timestamp) AS language,
        argMax(browser, timestamp) AS browser,
        argMax(operating_system, timestamp) AS operating_system,
        argMax(device_type, timestamp) AS device_type,
        argMin(hostname, timestamp) AS hostname,
        countIf(type = 'pageview') AS pageviews,
        countIf(type = 'custom_event') AS events,
        count(distinct session_id) AS sessions,
        max(timestamp) AS last_seen,
        min(timestamp) AS first_seen
    FROM events
    WHERE
        site_id = {siteId:Int32}
        ${timeStatement}
    GROUP BY effective_user_id
)
SELECT *
FROM AggregatedUsers
WHERE 1 = 1 ${filterStatement}
ORDER BY last_seen DESC
LIMIT {limit:Int32} OFFSET {offset:Int32}
  `;

  const countQuery = `
SELECT
    count(DISTINCT COALESCE(NULLIF(events.identified_user_id, ''), events.user_id)) AS total_count
FROM events
WHERE
    site_id = {siteId:Int32}
    ${filterStatement}
    ${timeStatement}
  `;

  try {
    const [result, countResult] = await Promise.all([
      clickhouse.query({
        query,
        format: "JSONEachRow",
        query_params: { siteId, limit: params.limit, offset },
      }),
      clickhouse.query({
        query: countQuery,
        format: "JSONEachRow",
        query_params: { siteId },
      }),
    ]);

    const data = await processResults<{
      effective_user_id: string;
      user_id: string;
      identified_user_id: string;
      country: string;
      region: string;
      city: string;
      language: string;
      browser: string;
      operating_system: string;
      device_type: string;
      hostname: string;
      pageviews: number;
      events: number;
      sessions: number;
      last_seen: string;
      first_seen: string;
    }>(result);

    const countData = await processResults<{ total_count: number }>(countResult);
    const totalCount = countData[0]?.total_count || 0;

    // Enrich with traits from Postgres
    const dataWithTraits = await enrichWithTraits(data, siteId);

    return reply.send(
      buildSuccessResponse(dataWithTraits, {
        siteId,
        from: params.start_date,
        to: params.end_date,
        timezone: params.time_zone,
        page: params.page,
        limit: params.limit,
        totalCount,
      })
    );
  } catch (error) {
    console.error("Error fetching public users:", error);
    return reply.status(500).send(buildErrorResponse("Failed to fetch users", 500));
  }
}

/**
 * POST /api/public/analytics/funnels
 * Analyzes funnel conversion with specified steps
 */
export async function getPublicFunnel(
  request: FastifyRequest<{
    Querystring: PublicApiQuerystring;
    Body: {
      steps: Array<{
        value: string;
        name?: string;
        type: "page" | "event";
        hostname?: string;
      }>;
    };
  }>,
  reply: FastifyReply
) {
  const parsed = parsePublicApiParams(request.query as Record<string, unknown>);
  if (!parsed.success) {
    return reply.status(400).send(buildErrorResponse(parsed.error, 400));
  }

  const params = parsed.data;
  const siteId = getResolvedSiteId(request);
  const { steps } = request.body;

  if (!steps || steps.length < 2) {
    return reply.status(400).send(buildErrorResponse("At least 2 steps are required for a funnel", 400));
  }

  const timeStatement = getTimeStatement({
    start_date: params.start_date,
    end_date: params.end_date,
    time_zone: params.time_zone,
  });

  const filterStatement = getFilterStatement(params.filters || "", siteId, timeStatement);

  // Build conditional statements for each step
  const stepConditions = steps.map((step) => {
    let condition = "";
    if (step.type === "page") {
      condition = `type = 'pageview' AND pathname = ${SqlString.escape(step.value)}`;
    } else {
      condition = `type = 'custom_event' AND event_name = ${SqlString.escape(step.value)}`;
    }
    if (step.hostname) {
      condition += ` AND hostname = ${SqlString.escape(step.hostname)}`;
    }
    return condition;
  });

  const query = `
    WITH
    SessionActions AS (
      SELECT
        session_id,
        timestamp,
        pathname,
        event_name,
        type,
        hostname
      FROM events
      WHERE
        site_id = {siteId:Int32}
        ${timeStatement}
        ${filterStatement}
    ),
    Step1 AS (
      SELECT DISTINCT
        session_id,
        min(timestamp) as step_time
      FROM SessionActions
      WHERE ${stepConditions[0]}
      GROUP BY session_id
    )
    ${steps
      .slice(1)
      .map(
        (_, index) => `
    , Step${index + 2} AS (
      SELECT DISTINCT
        s${index + 1}.session_id,
        min(sa.timestamp) as step_time
      FROM Step${index + 1} s${index + 1}
      JOIN SessionActions sa ON s${index + 1}.session_id = sa.session_id
      WHERE
        sa.timestamp > s${index + 1}.step_time
        AND ${stepConditions[index + 1]}
      GROUP BY s${index + 1}.session_id
    )
    `
      )
      .join("")}
    , StepCounts AS (
      ${steps
        .map(
          (step, index) => `
          SELECT
            ${index + 1} as step_number,
            ${SqlString.escape(step.name || step.value)} as step_name,
            count(DISTINCT session_id) as visitors
          FROM Step${index + 1}
        `
        )
        .join("\nUNION ALL\n")}
    )
    SELECT
      s1.step_number,
      s1.step_name,
      s1.visitors as visitors,
      round(s1.visitors * 100.0 / first_step.visitors, 2) as conversion_rate,
      CASE 
        WHEN s1.step_number = 1 THEN 0
        ELSE round((1 - (s1.visitors / prev_step.visitors)) * 100.0, 2)
      END as dropoff_rate
    FROM StepCounts s1
    CROSS JOIN (SELECT visitors FROM StepCounts WHERE step_number = 1) as first_step
    LEFT JOIN (
      SELECT step_number + 1 as next_step_number, visitors
      FROM StepCounts
      WHERE step_number < {stepNumber:Int32}
    ) as prev_step ON s1.step_number = prev_step.next_step_number
    ORDER BY s1.step_number
    `;

  try {
    const result = await clickhouse.query({
      query,
      format: "JSONEachRow",
      query_params: { siteId, stepNumber: steps.length },
    });

    const data = await processResults<{
      step_number: number;
      step_name: string;
      visitors: number;
      conversion_rate: number;
      dropoff_rate: number;
    }>(result);

    return reply.send(
      buildSuccessResponse(data, {
        siteId,
        from: params.start_date,
        to: params.end_date,
        timezone: params.time_zone,
      })
    );
  } catch (error) {
    console.error("Error executing public funnel query:", error);
    return reply.status(500).send(buildErrorResponse("Failed to execute funnel analysis", 500));
  }
}
