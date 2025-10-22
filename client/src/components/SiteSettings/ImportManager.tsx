"use client";

import { useState, useMemo, useRef } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { Upload, FileText, AlertCircle, CheckCircle2, Clock, Loader2, Database, Trash2 } from "lucide-react";
import { useGetSiteImports, useImportSiteData, useDeleteSiteImport } from "@/api/admin/import";
import { SplitDateRangePicker, DateRange } from "@/components/SplitDateRangePicker";

interface ImportManagerProps {
  siteId: number;
  disabled: boolean;
}

interface FileValidationError {
  type: "size" | "type" | "name";
  message: string;
}

const MAX_FILE_SIZE = 100 * 1024 * 1024; // 100MB
const ALLOWED_FILE_TYPES = ["text/csv"];
const ALLOWED_EXTENSIONS = [".csv"];
const DATA_SOURCES = [{ value: "umami", label: "Umami" }];

export function ImportManager({ siteId, disabled }: ImportManagerProps) {
  const [file, setFile] = useState<File | null>(null);
  const [source, setSource] = useState<string>("");
  const [dateRange, setDateRange] = useState<DateRange>({});
  const [fileError, setFileError] = useState<FileValidationError | null>(null);
  const [showConfirmDialog, setShowConfirmDialog] = useState<boolean>(false);
  const [deleteImportId, setDeleteImportId] = useState<string | null>(null);
  const [showDeleteDialog, setShowDeleteDialog] = useState<boolean>(false);

  const fileInputRef = useRef<HTMLInputElement>(null);

  const { data, isLoading, error } = useGetSiteImports(siteId);
  const mutation = useImportSiteData(siteId);
  const deleteMutation = useDeleteSiteImport(siteId);

  const resetFileInput = () => {
    setFile(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = "";
    }
  };

  const validateFile = (file: File): FileValidationError | null => {
    if (file.size > MAX_FILE_SIZE) {
      return {
        type: "size",
        message: `File size (${(file.size / 1024 / 1024).toFixed(1)}MB) exceeds the maximum limit of 100MB.`,
      };
    }

    if (!ALLOWED_FILE_TYPES.includes(file.type) && file.type !== "") {
      return {
        type: "type",
        message: "Invalid file type. Only CSV files are accepted.",
      };
    }

    const extension = "." + file.name.split(".").pop()?.toLowerCase();
    if (!ALLOWED_EXTENSIONS.includes(extension)) {
      return {
        type: "type",
        message: "Invalid file extension. Only .csv files are accepted.",
      };
    }

    if (file.name.length > 255) {
      return {
        type: "name",
        message: "Filename is too long. Please use a shorter filename.",
      };
    }

    return null;
  };

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = event.target.files?.[0];
    setFileError(null);

    if (selectedFile) {
      const validationError = validateFile(selectedFile);
      if (validationError) {
        setFileError(validationError);
        resetFileInput();
        return;
      }
      setFile(selectedFile);
    } else {
      setFile(null);
    }
  };

  const handleImportClick = () => {
    if (file && file.size > 50 * 1024 * 1024) {
      setShowConfirmDialog(true);
    } else {
      handleImport();
    }
  };

  const handleImport = () => {
    if (file && source) {
      const startDate = dateRange.startDate ? dateRange.startDate.toFormat("yyyy-MM-dd") : undefined;
      const endDate = dateRange.endDate ? dateRange.endDate.toFormat("yyyy-MM-dd") : undefined;

      mutation.mutate({
        file,
        source,
        startDate,
        endDate,
      });

      resetFileInput();
      setFileError(null);
      setSource("");
      setDateRange({});
      setShowConfirmDialog(false);
    }
  };

  const handleDeleteClick = (importId: string) => {
    setDeleteImportId(importId);
    setShowDeleteDialog(true);
  };

  const handleDeleteConfirm = () => {
    if (deleteImportId) {
      deleteMutation.mutate(deleteImportId);
      setDeleteImportId(null);
      setShowDeleteDialog(false);
    }
  };

  const getStatusInfo = (status: string) => {
    switch (status.toLowerCase()) {
      case "completed":
        return {
          color: "bg-green-100 text-green-800 border-green-200",
          icon: CheckCircle2,
          label: "Completed",
        };
      case "failed":
        return {
          color: "bg-red-100 text-red-800 border-red-200",
          icon: AlertCircle,
          label: "Failed",
        };
      case "processing":
        return {
          color: "bg-blue-100 text-blue-800 border-blue-200",
          icon: Loader2,
          label: "Processing",
        };
      case "pending":
        return {
          color: "bg-yellow-100 text-yellow-800 border-yellow-200",
          icon: Clock,
          label: "Pending",
        };
      default:
        return {
          color: "bg-gray-100 text-gray-800 border-gray-200",
          icon: Clock,
          label: status,
        };
    }
  };

  const sortedImports = useMemo(() => {
    if (!data?.data) {
      return [];
    }

    return [...data.data].sort((a, b) => {
      const aTime = new Date(a.startedAt).getTime();
      const bTime = new Date(b.startedAt).getTime();
      return bTime - aTime;
    });
  }, [data?.data]);

  const isImportDisabled = !file || !source || mutation.isPending || disabled || !!fileError;

  return (
    <div className="space-y-6">
      {/* Import Section */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Upload className="h-5 w-5" />
            Import Data
          </CardTitle>
          <CardDescription>Import data from other analytics platforms. Supports CSV files up to 100MB.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Data Source Selection */}
          <div className="space-y-2">
            <Label htmlFor="source" className="flex items-center gap-2">
              <Database className="h-4 w-4" />
              Data Source
            </Label>
            <Select value={source} onValueChange={setSource} disabled={disabled || mutation.isPending}>
              <SelectTrigger>
                <SelectValue placeholder="Select data source" />
              </SelectTrigger>
              <SelectContent>
                {DATA_SOURCES.map(dataSource => (
                  <SelectItem key={dataSource.value} value={dataSource.value}>
                    {dataSource.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {/* Date Range Picker */}
          <SplitDateRangePicker
            value={dateRange}
            onChange={setDateRange}
            label="Date Range (Optional)"
            disabled={disabled || mutation.isPending}
            showDescription={true}
            clearButtonText="Clear dates"
            className="space-y-2"
          />

          <Separator />

          {/* File Upload */}
          <div className="space-y-2">
            <Label htmlFor="file" className="flex items-center gap-2">
              <FileText className="h-4 w-4" />
              CSV File
            </Label>
            <Input
              ref={fileInputRef}
              type="file"
              accept=".csv"
              onChange={handleFileChange}
              disabled={disabled || mutation.isPending}
            />
          </div>

          {/* File Validation Error */}
          {fileError && (
            <Alert variant="destructive">
              <div className="flex items-center gap-2">
                <AlertCircle className="h-4 w-4" />
                <AlertDescription>{fileError.message}</AlertDescription>
              </div>
            </Alert>
          )}

          {/* Import Button */}
          <Button onClick={handleImportClick} disabled={isImportDisabled} className="w-full sm:w-auto">
            {mutation.isPending ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Importing...
              </>
            ) : (
              <>
                <Upload className="mr-2 h-4 w-4" />
                Import
              </>
            )}
          </Button>

          {/* Import Error */}
          {mutation.isError && (
            <Alert variant="destructive">
              <div className="flex items-center gap-2">
                <AlertCircle className="h-4 w-4" />
                <AlertDescription>
                  {mutation.error.message || "Failed to import file. Please try again."}
                </AlertDescription>
              </div>
            </Alert>
          )}

          {/* Success Message */}
          {mutation.isSuccess && (
            <Alert className="border-green-200 bg-green-50">
              <div className="flex items-center gap-2">
                <CheckCircle2 className="h-4 w-4 text-green-600" />
                <AlertDescription>File uploaded successfully and is being processed.</AlertDescription>
              </div>
            </Alert>
          )}

          {/* Delete Success Message */}
          {deleteMutation.isSuccess && (
            <Alert className="border-green-200 bg-green-50">
              <div className="flex items-center gap-2">
                <CheckCircle2 className="h-4 w-4 text-green-600" />
                <AlertDescription>Import deleted successfully.</AlertDescription>
              </div>
            </Alert>
          )}

          {/* Delete Error Message */}
          {deleteMutation.isError && (
            <Alert variant="destructive">
              <div className="flex items-center gap-2">
                <AlertCircle className="h-4 w-4" />
                <AlertDescription>
                  {deleteMutation.error.message || "Failed to delete import. Please try again."}
                </AlertDescription>
              </div>
            </Alert>
          )}
        </CardContent>
      </Card>

      {/* Import History */}
      <Card>
        <CardHeader>
          <CardTitle>Import History</CardTitle>
          <CardDescription>Track the status of your data imports</CardDescription>
        </CardHeader>
        <CardContent>
          {isLoading && !data ? (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="h-6 w-6 animate-spin mr-2" />
              <span>Loading import history...</span>
            </div>
          ) : error ? (
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertDescription>Failed to load import history. Please try refreshing the page.</AlertDescription>
            </Alert>
          ) : !data?.data?.length ? (
            <div className="text-center py-8 text-muted-foreground">
              <FileText className="h-8 w-8 mx-auto mb-2 opacity-50" />
              <p>No imports yet</p>
              <p className="text-sm">Upload a CSV file to get started</p>
            </div>
          ) : (
            <div className="rounded-md">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>File</TableHead>
                    <TableHead>Source</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead className="text-right">Events</TableHead>
                    <TableHead className="text-right">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {sortedImports.map(imp => {
                    const statusInfo = getStatusInfo(imp.status);
                    const StatusIcon = statusInfo.icon;

                    return (
                      <TableRow key={imp.importId}>
                        <TableCell className="font-medium">
                          <div className="max-w-[200px] truncate" title={imp.fileName}>
                            {imp.fileName}
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge variant="outline" className="capitalize">
                            {imp.source}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <div className="space-y-1">
                            <Badge className={`${statusInfo.color} flex items-center gap-1 w-fit`}>
                              <StatusIcon className={`h-3 w-3 ${imp.status === "processing" ? "animate-spin" : ""}`} />
                              {statusInfo.label}
                            </Badge>
                            {imp.errorMessage && (
                              <p className="text-xs text-red-600 max-w-[200px] truncate" title={imp.errorMessage}>
                                {imp.errorMessage}
                              </p>
                            )}
                          </div>
                        </TableCell>
                        <TableCell className="text-right">{imp.importedEvents.toLocaleString()}</TableCell>
                        <TableCell className="text-right">
                          {(imp.status === "completed" || imp.status === "failed") && (
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => handleDeleteClick(imp.importId)}
                              disabled={disabled || deleteMutation.isPending}
                              className="h-8 w-8 p-0"
                            >
                              {deleteMutation.isPending && deleteMutation.variables === imp.importId ? (
                                <Loader2 className="h-4 w-4 animate-spin" />
                              ) : (
                                <Trash2 className="h-4 w-4" />
                              )}
                            </Button>
                          )}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Confirmation Dialog */}
      <AlertDialog open={showConfirmDialog} onOpenChange={setShowConfirmDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Confirm Large File Import</AlertDialogTitle>
            <AlertDialogDescription>
              You're about to import a large file. This may take several minutes to process. Are you sure you want to
              continue?
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={handleImport}>Yes, Import File</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Delete Confirmation Dialog */}
      <AlertDialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Import</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete this import? This action cannot be undone. The import data and associated
              files will be permanently removed.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={handleDeleteConfirm} className="bg-red-600 hover:bg-red-700">
              Delete Import
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
