import { db } from "../../db/postgres/postgres.js";
import { eq, desc, sql } from "drizzle-orm";
import { importStatus } from "../../db/postgres/schema.js";
import { DateTime } from "luxon";

type SelectImportStatus = typeof importStatus.$inferSelect;
type InsertImportStatus = typeof importStatus.$inferInsert;

export class ImportStatusManager {
  static async createImportStatus(data: InsertImportStatus): Promise<void> {
    try {
      await db.insert(importStatus).values(data);
    } catch (error) {
      console.error("Failed to create import status:", error);
      throw new Error(`Failed to create import status: ${error instanceof Error ? error.message : "Unknown error"}`);
    }
  }

  static async updateStatus(
    importId: string,
    status: InsertImportStatus["status"],
    errorMessage?: string
  ): Promise<void> {
    const completedAt = status === "completed" || status === "failed" ? DateTime.utc().toISO() : null;

    try {
      await db
        .update(importStatus)
        .set({ status, errorMessage, completedAt })
        .where(eq(importStatus.importId, importId));
    } catch (error) {
      console.error(`Failed to update import status for ${importId}:`, error);
      throw new Error(`Failed to update import status: ${error instanceof Error ? error.message : "Unknown error"}`);
    }
  }

  static async updateProgress(importId: string, importedEvents: number): Promise<void> {
    try {
      await db
        .update(importStatus)
        .set({
          importedEvents: sql`${importStatus.importedEvents} + ${importedEvents}`,
        })
        .where(eq(importStatus.importId, importId));
    } catch (error) {
      console.error(`Failed to update import progress for ${importId}:`, error);
      throw new Error(`Failed to update import progress: ${error instanceof Error ? error.message : "Unknown error"}`);
    }
  }

  static async getImportsForSite(siteId: number, limit = 10): Promise<SelectImportStatus[]> {
    try {
      return await db.query.importStatus.findMany({
        where: eq(importStatus.siteId, siteId),
        orderBy: [desc(importStatus.startedAt)],
        limit,
      });
    } catch (error) {
      console.error(`Failed to get imports for site ${siteId}:`, error);
      throw new Error(`Failed to get imports for site: ${error instanceof Error ? error.message : "Unknown error"}`);
    }
  }

  static async deleteImport(importId: string): Promise<void> {
    try {
      await db.delete(importStatus).where(eq(importStatus.importId, importId));
    } catch (error) {
      console.error(`Failed to delete import ${importId}:`, error);
      throw new Error(`Failed to delete import: ${error instanceof Error ? error.message : "Unknown error"}`);
    }
  }

  static async getImportById(importId: string): Promise<SelectImportStatus | undefined> {
    try {
      return await db.query.importStatus.findFirst({
        where: eq(importStatus.importId, importId),
      });
    } catch (error) {
      console.error(`Failed to get import ${importId}:`, error);
      throw new Error(`Failed to get import: ${error instanceof Error ? error.message : "Unknown error"}`);
    }
  }
}
