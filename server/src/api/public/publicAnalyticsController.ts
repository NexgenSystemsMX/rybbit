import { FastifyReply, FastifyRequest } from "fastify";
import { buildSuccessResponse, buildErrorResponse, parsePublicApiParams } from "./publicApiHelpers.js";
import { getResolvedSiteId } from "./publicApiMiddleware.js";

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

  return reply.send(
    buildSuccessResponse(
      { sessions: 0, pageviews: 0, users: 0, pages_per_session: 0, bounce_rate: 0, session_duration: 0 },
      {
        siteId,
        from: params.start_date,
        to: params.end_date,
        timezone: params.time_zone,
      }
    )
  );
}

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

  return reply.send(
    buildSuccessResponse([], {
      siteId,
      from: params.start_date,
      to: params.end_date,
      timezone: params.time_zone,
    })
  );
}

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

  return reply.send(
    buildSuccessResponse([], {
      siteId,
      from: params.start_date,
      to: params.end_date,
      timezone: params.time_zone,
      page: params.page,
      limit: params.limit,
      totalCount: 0,
    })
  );
}

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

  return reply.send(
    buildSuccessResponse([], {
      siteId,
      from: params.start_date,
      to: params.end_date,
      timezone: params.time_zone,
      page: params.page,
      limit: params.limit,
      totalCount: 0,
    })
  );
}

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

  return reply.send(
    buildSuccessResponse([], {
      siteId,
      from: params.start_date,
      to: params.end_date,
      timezone: params.time_zone,
      page: params.page,
      limit: params.limit,
    })
  );
}

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

  return reply.send(
    buildSuccessResponse([], {
      siteId,
      from: params.start_date,
      to: params.end_date,
      timezone: params.time_zone,
      page: params.page,
      limit: params.limit,
      totalCount: 0,
    })
  );
}

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

  return reply.send(
    buildSuccessResponse([], {
      siteId,
      from: params.start_date,
      to: params.end_date,
      timezone: params.time_zone,
    })
  );
}