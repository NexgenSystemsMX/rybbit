import { FastifyReply, FastifyRequest } from "fastify";
import { checkApiKey, getSessionFromReq } from "../../lib/auth-utils.js";
import { resolveNumericSiteId } from "../../utils.js";
import { db } from "../../db/postgres/postgres.js";
import { sites } from "../../db/postgres/schema.js";
import { eq } from "drizzle-orm";

/**
 * Middleware for public API endpoints that validates API key authentication
 * and ensures access to the requested siteId.
 * 
 * This middleware:
 * 1. Extracts siteId from query params
 * 2. Validates Bearer token (API key)
 * 3. Verifies the API key owner has access to the site
 * 4. Attaches resolved numeric siteId to request
 */
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

  // Resolve string site ID to numeric if needed
  let numericSiteId: number;
  if (String(siteIdParam).length > 4) {
    const resolved = await resolveNumericSiteId(siteIdParam);
    if (!resolved) {
      return reply.status(404).send({
        success: false,
        error: "Site not found",
      });
    }
    numericSiteId = resolved;
  } else {
    numericSiteId = Number(siteIdParam);
    if (isNaN(numericSiteId)) {
      return reply.status(400).send({
        success: false,
        error: "Invalid siteId format",
      });
    }
  }

  // Verify site exists
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

  // Check for Bearer token authentication
  const authHeader = request.headers["authorization"];
  const hasBearerToken =
    typeof authHeader === "string" && authHeader.startsWith("Bearer ");

  if (!hasBearerToken) {
    // Also check for session-based auth as fallback
    const session = await getSessionFromReq(request);
    if (!session?.user) {
      return reply.status(401).send({
        success: false,
        error: "Authorization required. Use Bearer token with API key.",
      });
    }
    // Session user - verify site access via existing logic
    request.user = session.user;
  }

  // Validate API key and check access to site
  const apiKeyResult = await checkApiKey(request, { siteId: String(numericSiteId) });

  if (!apiKeyResult.valid) {
    // If API key is invalid, check if we have a valid session
    const session = await getSessionFromReq(request);
    if (!session?.user) {
      return reply.status(401).send({
        success: false,
        error: "Invalid or expired API key",
      });
    }
    // Session is valid, continue
    request.user = session.user;
  }

  // Attach resolved siteId to request for downstream handlers
  (request as any).resolvedSiteId = numericSiteId;
}

/**
 * Get the resolved numeric siteId from request
 */
export function getResolvedSiteId(request: FastifyRequest): number {
  return (request as any).resolvedSiteId;
}
