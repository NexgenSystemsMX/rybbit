import cors from "@fastify/cors";
import fastifyStatic from "@fastify/static";
import { toNodeHandler } from "better-auth/node";
import Fastify, { type FastifyInstance, type FastifyReply, type FastifyRequest } from "fastify";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  collectTelemetry,
  getAdminOrganizations,
  getAdminServiceEventCount,
  getAdminSites,
} from "./api/admin/index.js";
import {
  createFunnel,
  createGoal,
  deleteFunnel,
  deleteGoal,
  getErrorBucketed,
  getErrorEvents,
  getErrorNames,
  getEventNames,
  getEventProperties,
  getEvents,
  getFunnel,
  getFunnelStepSessions,
  getFunnels,
  getGoalSessions,
  getGoals,
  getJourneys,
  getLiveUsercount,
  getMetric,
  getOrgEventCount,
  getOutboundLinks,
  getOverview,
  getOverviewBucketed,
  getPageTitles,
  getPerformanceByDimension,
  getPerformanceOverview,
  getPerformanceTimeSeries,
  getRetention,
  getSession,
  getSessionLocations,
  getSessions,
  getUserInfo,
  getUserSessionCount,
  getUsers,
  updateGoal,
} from "./api/analytics/index.js";
import { getConfig } from "./api/getConfig.js";
import {
  connectGSC,
  disconnectGSC,
  getGSCData,
  getGSCStatus,
  gscCallback,
  selectGSCProperty,
} from "./api/gsc/index.js";
import {
  deleteSessionReplay,
  getSessionReplayEvents,
  getSessionReplays,
  recordSessionReplay,
} from "./api/sessionReplay/index.js";
import {
  addSite,
  batchImportEvents,
  createSiteImport,
  deleteSite,
  deleteSiteImport,
  getSite,
  getSiteExcludedCountries,
  getSiteExcludedIPs,
  getSiteHasData,
  getSiteImports,
  getSiteIsPublic,
  getSitePrivateLinkConfig,
  getSitesFromOrg,
  getTrackingConfig,
  updateSiteConfig,
  updateSitePrivateLinkConfig,
} from "./api/sites/index.js";
import {
  createCheckoutSession,
  createPortalSession,
  getSubscription,
  handleWebhook,
  previewSubscriptionUpdate,
  updateSubscription,
} from "./api/stripe/index.js";
import {
  addUserToOrganization,
  createApiKey,
  deleteApiKey,
  getUserOrganizations,
  listApiKeys,
  listOrganizationMembers,
  updateAccountSettings,
} from "./api/user/index.js";
import { initializeClickhouse } from "./db/clickhouse/clickhouse.js";
import { initPostgres } from "./db/postgres/initPostgres.js";
import { mapHeaders } from "./lib/auth-utils.js";
import { auth } from "./lib/auth.js";
import { IS_CLOUD } from "./lib/const.js";
import { trackEvent } from "./services/tracker/trackEvent.js";
import { handleIdentify } from "./services/tracker/identifyService.js";
// need to import telemetry service here to start it
import { telemetryService } from "./services/telemetryService.js";
import { weeklyReportService } from "./services/weekyReports/weeklyReportService.js";
import {
  requireAuth,
  requireAdmin,
  requireSiteAccess,
  requireSiteAdminAccess,
  allowPublicSiteAccess,
  requireOrgMember,
  requireOrgAdminFromParams,
  resolveSiteId,
} from "./lib/auth-middleware.js";

// Pre-composed middleware chains for common auth patterns
// Cast as any to work around Fastify's type inference limitations with preHandler
const publicSite = { preHandler: [resolveSiteId, allowPublicSiteAccess] as any };
const authSite = { preHandler: [resolveSiteId, requireSiteAccess] as any };
const adminSite = { preHandler: [resolveSiteId, requireSiteAdminAccess] as any };
const authOnly = { preHandler: [requireAuth] as any };
const adminOnly = { preHandler: [requireAdmin] as any };
const orgMember = { preHandler: [requireOrgMember] as any };
const orgAdminParams = { preHandler: [requireOrgAdminFromParams] as any };

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const hasAxiom = !!(process.env.AXIOM_DATASET && process.env.AXIOM_TOKEN);

const server = Fastify({
  logger: {
    level: process.env.LOG_LEVEL || (process.env.NODE_ENV === "development" ? "debug" : "info"),
    transport:
      process.env.NODE_ENV === "production" && IS_CLOUD && hasAxiom
        ? {
            targets: [
              // Send to Axiom
              {
                target: "@axiomhq/pino",
                level: process.env.LOG_LEVEL || "info",
                options: {
                  dataset: process.env.AXIOM_DATASET,
                  token: process.env.AXIOM_TOKEN,
                },
              },
              // Pretty print to stdout for Docker logs
              {
                target: "pino-pretty",
                level: process.env.LOG_LEVEL || "info",
                options: {
                  colorize: true,
                  singleLine: true,
                  translateTime: "HH:MM:ss",
                  ignore: "pid,hostname,name",
                  destination: 1, // stdout
                },
              },
            ],
          }
        : process.env.NODE_ENV === "development"
          ? {
              target: "pino-pretty",
              options: {
                colorize: true,
                singleLine: true,
                translateTime: "HH:MM:ss",
                ignore: "pid,hostname,name",
              },
            }
          : undefined, // Production without Axiom - plain JSON to stdout
    serializers: {
      req(request) {
        return {
          method: request.method,
          url: request.url,
          path: request.url,
          parameters: request.params,
        };
      },
      res(reply) {
        return {
          statusCode: reply.statusCode,
        };
      },
    },
  },
  maxParamLength: 1500,
  trustProxy: true,
  bodyLimit: 10 * 1024 * 1024, // 10MB limit for session replay data
});

server.register(cors, {
  origin: (_origin, callback) => {
    callback(null, true);
  },
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
  allowedHeaders: ["Content-Type", "Authorization", "X-Requested-With", "x-captcha-response", "x-private-key"],
  credentials: true,
});

// Serve static files
server.register(fastifyStatic, {
  root: join(__dirname, "../public"),
  prefix: "/", // or whatever prefix you need
});

server.register(
  async (fastify, options) => {
    await fastify.register(fastify => {
      const authHandler = toNodeHandler(options.auth);

      fastify.addContentTypeParser(
        "application/json",
        /* c8 ignore next 3 */
        (_request, _payload, done) => {
          done(null, null);
        }
      );

      fastify.all("/api/auth/*", async (request, reply: any) => {
        reply.raw.setHeaders(mapHeaders(reply.getHeaders()));
        await authHandler(request.raw, reply.raw);
      });
      fastify.all("/auth/*", async (request, reply: any) => {
        reply.raw.setHeaders(mapHeaders(reply.getHeaders()));
        await authHandler(request.raw, reply.raw);
      });
    });
  },
  { auth: auth! }
);

// Serve analytics scripts with generic names to avoid ad-blocker detection
server.get("/api/script.js", async (_, reply) => reply.sendFile("script.js"));
server.get("/api/replay.js", async (_, reply) => reply.sendFile("rrweb.min.js"));
server.get("/api/metrics.js", async (_, reply) => reply.sendFile("web-vitals.iife.js"));

// Domain-specific route plugins
async function analyticsRoutes(fastify: FastifyInstance) {
  // WEB & PRODUCT ANALYTICS

  // This endpoint gets called a lot so we don't want to log it
  fastify.get("/live-user-count/:siteId", { logLevel: "silent", ...publicSite }, getLiveUsercount);
  fastify.get("/overview/:siteId", publicSite, getOverview);
  fastify.get("/overview-bucketed/:siteId", publicSite, getOverviewBucketed);
  fastify.get("/metric/:siteId", publicSite, getMetric);
  fastify.get("/page-titles/:siteId", publicSite, getPageTitles);
  fastify.get("/error-names/:siteId", publicSite, getErrorNames);
  fastify.get("/error-events/:siteId", publicSite, getErrorEvents);
  fastify.get("/error-bucketed/:siteId", publicSite, getErrorBucketed);
  fastify.get("/retention/:siteId", publicSite, getRetention);
  fastify.get("/site-has-data/:siteId", publicSite, getSiteHasData);
  fastify.get("/site-is-public/:siteId", publicSite, getSiteIsPublic);
  fastify.get("/sessions/:siteId", publicSite, getSessions);
  fastify.get("/sessions/:sessionId/:siteId", publicSite, getSession);
  fastify.get("/events/:siteId", publicSite, getEvents);
  fastify.get("/users/:siteId", publicSite, getUsers);
  fastify.get("/users/session-count/:siteId", publicSite, getUserSessionCount);
  fastify.get("/users/:userId/:siteId", publicSite, getUserInfo);
  fastify.get("/session-locations/:siteId", publicSite, getSessionLocations);
  fastify.get("/funnels/:siteId", publicSite, getFunnels);
  fastify.get("/journeys/:siteId", publicSite, getJourneys);
  fastify.post("/funnels/analyze/:siteId", publicSite, getFunnel);
  fastify.post("/funnels/:stepNumber/sessions/:siteId", publicSite, getFunnelStepSessions);
  fastify.post("/funnels/:siteId", authSite, createFunnel);
  fastify.delete("/funnels/:funnelId/:siteId", authSite, deleteFunnel);
  fastify.get("/goals/:siteId", publicSite, getGoals);
  fastify.get("/goals/:goalId/sessions/:siteId", publicSite, getGoalSessions);
  fastify.post("/goals/:siteId", authSite, createGoal);
  fastify.delete("/goals/:goalId/:siteId", authSite, deleteGoal);
  fastify.put("/goals/:goalId/:siteId", authSite, updateGoal);
  fastify.get("/events/names/:siteId", publicSite, getEventNames);
  fastify.get("/events/properties/:siteId", publicSite, getEventProperties);
  fastify.get("/events/outbound/:siteId", publicSite, getOutboundLinks);
  fastify.get("/org-event-count/:organizationId", orgMember, getOrgEventCount);

  // Performance Analytics
  fastify.get("/performance/overview/:siteId", publicSite, getPerformanceOverview);
  fastify.get("/performance/time-series/:siteId", publicSite, getPerformanceTimeSeries);
  fastify.get("/performance/by-dimension/:siteId", publicSite, getPerformanceByDimension);
}

async function sessionReplayRoutes(fastify: FastifyInstance) {
  // Session Replay
  fastify.post("/session-replay/record/:siteId", recordSessionReplay); // Public - tracking endpoint
  fastify.get("/session-replay/list/:siteId", publicSite, getSessionReplays);
  fastify.get("/session-replay/:sessionId/:siteId", publicSite, getSessionReplayEvents);
  fastify.delete("/session-replay/:sessionId/:siteId", authSite, deleteSessionReplay);
}

async function sitesRoutes(fastify: FastifyInstance) {
  // Sites
  fastify.get("/sites/:siteId", publicSite, getSite);
  fastify.put("/sites/:siteId/config", adminSite, updateSiteConfig);
  fastify.delete("/sites/:siteId", adminSite, deleteSite);
  fastify.get("/sites/:siteId/private-link-config", adminSite, getSitePrivateLinkConfig);
  fastify.post("/sites/:siteId/private-link-config", adminSite, updateSitePrivateLinkConfig);
  fastify.get("/site/tracking-config/:siteId", getTrackingConfig); // Public - used by tracking script
  fastify.get("/sites/:siteId/excluded-ips", authSite, getSiteExcludedIPs);
  fastify.get("/sites/:siteId/excluded-countries", authSite, getSiteExcludedCountries);

  // Site Imports
  fastify.get("/sites/:siteId/imports", adminSite, getSiteImports);
  fastify.post("/sites/:siteId/imports", adminSite, createSiteImport);
  fastify.post("/sites/:siteId/imports/:importId/events", adminSite, batchImportEvents);
  fastify.delete("/sites/:siteId/imports/:importId", adminSite, deleteSiteImport);
}

async function organizationsRoutes(fastify: FastifyInstance) {
  // Organizations
  fastify.get("/organizations/:organizationId/sites", orgMember, getSitesFromOrg);
  fastify.post("/organizations/:organizationId/sites", orgAdminParams, addSite);
  fastify.get("/organizations/:organizationId/members", orgMember, listOrganizationMembers);
  fastify.post("/organizations/:organizationId/members", orgMember, addUserToOrganization);
}

async function userRoutes(fastify: FastifyInstance) {
  // User
  fastify.get("/config", getConfig); // Public - returns app config
  fastify.get("/user/organizations", authOnly, getUserOrganizations);
  fastify.post("/user/account-settings", authOnly, updateAccountSettings);
  fastify.get("/user/api-keys", authOnly, listApiKeys);
  fastify.post("/user/api-keys", authOnly, createApiKey);
  fastify.delete("/user/api-keys/:keyId", authOnly, deleteApiKey);
}

async function gscRoutes(fastify: FastifyInstance) {
  // GOOGLE SEARCH CONSOLE
  fastify.get("/gsc/connect/:siteId", authSite, connectGSC);
  fastify.get("/gsc/callback", gscCallback); // Public - OAuth callback
  fastify.get("/gsc/status/:siteId", publicSite, getGSCStatus);
  fastify.delete("/gsc/disconnect/:siteId", authSite, disconnectGSC);
  fastify.post("/gsc/select-property/:siteId", authSite, selectGSCProperty);
  fastify.get("/gsc/data/:siteId", publicSite, getGSCData);
}

async function stripeAdminRoutes(fastify: FastifyInstance) {
  // STRIPE & ADMIN
  if (IS_CLOUD) {
    // Stripe Routes
    fastify.post("/stripe/create-checkout-session", authOnly, createCheckoutSession);
    fastify.post("/stripe/create-portal-session", authOnly, createPortalSession);
    fastify.post("/stripe/preview-subscription-update", authOnly, previewSubscriptionUpdate);
    fastify.post("/stripe/update-subscription", authOnly, updateSubscription);
    fastify.get("/stripe/subscription", authOnly, getSubscription);
    fastify.post("/stripe/webhook", { config: { rawBody: true } }, handleWebhook); // Public - Stripe webhook

    // Admin Routes
    fastify.get("/admin/sites", adminOnly, getAdminSites);
    fastify.get("/admin/organizations", adminOnly, getAdminOrganizations);
    fastify.get("/admin/service-event-count", adminOnly, getAdminServiceEventCount);
    fastify.post("/admin/telemetry", collectTelemetry); // Public - telemetry collection

    // AppSumo Routes
    const { activateAppSumoLicense, handleAppSumoWebhook } = await import("./api/as/index.js");

    fastify.post("/as/activate", authOnly, activateAppSumoLicense);
    fastify.post("/as/webhook", handleAppSumoWebhook); // Public - AppSumo webhook
  }
}

// Main API routes plugin - registers all domain plugins
async function apiRoutes(fastify: FastifyInstance) {
  await fastify.register(analyticsRoutes);
  await fastify.register(sessionReplayRoutes);
  await fastify.register(sitesRoutes);
  await fastify.register(organizationsRoutes);
  await fastify.register(userRoutes);
  await fastify.register(gscRoutes);
  await fastify.register(stripeAdminRoutes);

  // Health check
  fastify.get("/health", { logLevel: "silent" }, (_: FastifyRequest, reply: FastifyReply) => reply.send("OK"));
}

server.post("/api/track", trackEvent);
server.post("/api/identify", handleIdentify);

// Register API routes with /api prefix
server.register(apiRoutes, { prefix: "/api" });

const start = async () => {
  try {
    console.info("Starting server...");
    await Promise.all([initializeClickhouse(), initPostgres()]);

    telemetryService.startTelemetryCron();
    if (IS_CLOUD) {
      weeklyReportService.startWeeklyReportCron();
    }

    // Start the server first
    await server.listen({ port: 3001, host: "0.0.0.0" });
    server.log.info("Server is listening on http://0.0.0.0:3001");

    // Test Axiom logging
    if (hasAxiom) {
      server.log.info({ axiom: true, dataset: process.env.AXIOM_DATASET }, "Axiom logging is configured");
    }

    // if (process.env.NODE_ENV === "production") {
    //   // Initialize uptime monitoring service in the background (non-blocking)
    //   uptimeService
    //     .initialize()
    //     .then(() => {
    //       server.log.info("Uptime monitoring service initialized successfully");
    //     })
    //     .catch((error) => {
    //       server.log.error("Failed to initialize uptime service:", error);
    //       // Continue running without uptime monitoring
    //     });
    // }
  } catch (err) {
    server.log.error(err);
    process.exit(1);
  }
};

start();

// Graceful shutdown
let isShuttingDown = false;

const shutdown = async (signal: string) => {
  if (isShuttingDown) {
    server.log.warn(`${signal} received during shutdown, forcing exit...`);
    process.exit(1);
  }

  isShuttingDown = true;
  server.log.info(`${signal} received, shutting down gracefully...`);

  // Set a timeout to force exit if shutdown takes too long
  const forceExitTimeout = setTimeout(() => {
    server.log.error("Shutdown timeout exceeded, forcing exit...");
    process.exit(1);
  }, 10000); // 10 second timeout

  try {
    // Stop accepting new connections
    await server.close();
    server.log.info("Server closed");

    // Shutdown uptime service
    // await uptimeService.shutdown();
    // server.log.info("Uptime service shut down");

    // Clear the timeout since we're done
    clearTimeout(forceExitTimeout);

    process.exit(0);
  } catch (error) {
    server.log.error(error, "Error during shutdown");
    clearTimeout(forceExitTimeout);
    process.exit(1);
  }
};

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

declare module "fastify" {
  interface FastifyRequest {
    user?: any; // Or define a more specific user type
  }
}
