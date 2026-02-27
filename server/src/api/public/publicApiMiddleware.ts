import { FastifyReply, FastifyRequest } from "fastify";
import { checkApiKey, getSessionFromReq } from "../../lib/auth-utils.js";
import { db } from "../../db/postgres/postgres.js";
import { sites } from "../../db/postgres/schema.js";
import { eq } from "drizzle-orm";

export async function requirePublicApiKey(
  request: FastifyRequest<{ Querystring: { siteId?: string } }>,
  reply: FastifyReply
): Promise<void> {
  const siteIdParam = request.query.siteId;

  if (!siteIdParam) {
    return reply.status(400).send({
      success: false,
      error: "siteId query parameter is required",
    });
  }

  const numericSiteId = Number(siteIdParam);
  if (isNaN(numericSiteId) || numericSiteId <= 0) {
    return reply.status(400).send({
      success: false,
      error: "Invalid siteId format. Must be a positive number.",
    });
  }

  const siteRecord = await db
    .select({ siteId: sites.siteId, organizationId: sites.organizationId })
    .from(sites)
    .where(eq(sites.siteId, numericSiteId))
    .limit(1);

  if (siteRecord.length === 0) {
    return reply.status(404).send({
      success: false,
      error: "Site not found",
    });
  }

  const authHeader = request.headers["authorization"];
  const hasBearerToken =
    typeof authHeader === "string" && authHeader.startsWith("Bearer ");

  if (!hasBearerToken) {
    const session = await getSessionFromReq(request);
    if (!session?.user) {
      return reply.status(401).send({
        success: false,
        error: "Authorization required. Use Bearer token with API key.",
      });
    }
    request.user = session.user;
  }

  const apiKeyResult = await checkApiKey(request, { siteId: String(numericSiteId) });

  if (!apiKeyResult.valid) {
    const session = await getSessionFromReq(request);
    if (!session?.user) {
      return reply.status(401).send({
        success: false,
        error: "Invalid or expired API key",
      });
    }
    request.user = session.user;
  }

  (request as any).resolvedSiteId = numericSiteId;
}

export function getResolvedSiteId(request: FastifyRequest): number {
  return (request as any).resolvedSiteId;
}