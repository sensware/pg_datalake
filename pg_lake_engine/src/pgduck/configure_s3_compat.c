/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * configure_s3_compat.c -- PostgreSQL function that registers an
 * S3-compatible on-premises object store (MinIO, Ceph RadosGW, etc.)
 * with pgduck_server.
 *
 * Users call lake_engine.configure_s3_compat() from a regular PostgreSQL
 * connection instead of having to connect to pgduck_server directly.
 */

#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include "pg_lake/pgduck/client.h"


/*
 * ValidateSecretName checks that a DuckDB secret name contains only
 * characters that are safe to embed as a bare identifier: letters,
 * digits, underscores, or hyphens.  We do not quote the name in the
 * generated SQL, so any other character would be a syntax or injection
 * risk.
 */
static void
ValidateSecretName(const char *name)
{
	if (name[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("secret name must not be empty")));

	for (int i = 0; name[i] != '\0'; i++)
	{
		char		c = name[i];

		if (!((c >= 'a' && c <= 'z') ||
			  (c >= 'A' && c <= 'Z') ||
			  (c >= '0' && c <= '9') ||
			  c == '_' || c == '-'))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid secret name \"%s\": "
							"may only contain letters, digits, underscores, or hyphens",
							name)));
	}
}


PG_FUNCTION_INFO_V1(configure_s3_compat);

/*
 * configure_s3_compat builds and executes a DuckDB
 * CREATE OR REPLACE SECRET statement for an S3-compatible endpoint.
 *
 * SQL signature (see pg_lake_engine--3.3--3.4.sql):
 *
 *   lake_engine.configure_s3_compat(
 *       name      text,
 *       endpoint  text,
 *       key_id    text,
 *       secret    text,
 *       scope     text    DEFAULT NULL,
 *       use_ssl   boolean DEFAULT false,
 *       region    text    DEFAULT 'us-east-1',
 *       url_style text    DEFAULT 'path'
 *   ) RETURNS void
 */
Datum
configure_s3_compat(PG_FUNCTION_ARGS)
{
	/* required arguments */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) ||
		PG_ARGISNULL(2) || PG_ARGISNULL(3))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("name, endpoint, key_id, and secret must not be NULL")));

	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *endpoint = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *key_id = text_to_cstring(PG_GETARG_TEXT_PP(2));
	char	   *secret = text_to_cstring(PG_GETARG_TEXT_PP(3));

	/* optional arguments with defaults */
	bool		use_ssl = PG_ARGISNULL(5) ? false : PG_GETARG_BOOL(5);
	char	   *region = PG_ARGISNULL(6) ? "us-east-1"
		: text_to_cstring(PG_GETARG_TEXT_PP(6));
	char	   *url_style = PG_ARGISNULL(7) ? "path"
		: text_to_cstring(PG_GETARG_TEXT_PP(7));

	/* validate inputs */
	ValidateSecretName(name);

	if (strcmp(url_style, "path") != 0 && strcmp(url_style, "vhost") != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("url_style must be 'path' or 'vhost'")));

	/* build the DuckDB CREATE OR REPLACE SECRET command */
	StringInfoData cmd;

	initStringInfo(&cmd);

	/*
	 * The secret name is used as a bare DuckDB identifier; all other
	 * user-supplied strings are passed through quote_literal_cstr() to
	 * prevent SQL injection.
	 */
	appendStringInfo(&cmd,
					 "CREATE OR REPLACE SECRET %s ("
					 " TYPE S3,"
					 " KEY_ID %s,"
					 " SECRET %s,"
					 " ENDPOINT %s,"
					 " URL_STYLE %s,"
					 " USE_SSL %s,"
					 " REGION %s",
					 name,
					 quote_literal_cstr(key_id),
					 quote_literal_cstr(secret),
					 quote_literal_cstr(endpoint),
					 quote_literal_cstr(url_style),
					 use_ssl ? "true" : "false",
					 quote_literal_cstr(region));

	/* optional SCOPE narrows which s3:// URLs the secret applies to */
	if (!PG_ARGISNULL(4))
	{
		char	   *scope = text_to_cstring(PG_GETARG_TEXT_PP(4));

		appendStringInfo(&cmd, ", SCOPE %s", quote_literal_cstr(scope));
	}

	appendStringInfoChar(&cmd, ')');

	ExecuteCommandInPGDuck(cmd.data);

	PG_RETURN_VOID();
}
