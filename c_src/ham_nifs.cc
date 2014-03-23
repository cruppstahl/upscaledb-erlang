/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#include <string.h>
#include <stdio.h>

#include "erl_nif_compat.h"
#include "ham/hamsterdb.h"

ERL_NIF_TERM g_atom_ok;
ERL_NIF_TERM g_atom_error;
ERL_NIF_TERM g_atom_key_not_found;
ERL_NIF_TERM g_atom_duplicate_key;
ErlNifResourceType *g_ham_env_resource;
ErlNifResourceType *g_ham_db_resource;
ErlNifResourceType *g_ham_txn_resource;
ErlNifResourceType *g_ham_cursor_resource;

struct env_wrapper {
  ham_env_t *env;
  bool is_closed;
};

struct db_wrapper {
  ham_db_t *db;
  bool is_closed;
};

struct txn_wrapper {
  ham_txn_t *txn;
  bool is_closed;
};

struct cursor_wrapper {
  ham_cursor_t *cursor;
  bool is_closed;
};

#define MAX_PARAMETERS 64
#define MAX_STRING 2048

static ERL_NIF_TERM
status_to_atom(ErlNifEnv *env, ham_status_t st)
{
  switch (st) {
    case HAM_SUCCESS:
      return (g_atom_ok);
    case HAM_INV_RECORD_SIZE:
      return (enif_make_atom(env, "record_size"));
    case HAM_INV_KEY_SIZE:
      return (enif_make_atom(env, "key_size"));
    case HAM_INV_PAGE_SIZE:
      return (enif_make_atom(env, "page_size"));
    case HAM_OUT_OF_MEMORY:
      return (enif_make_atom(env, "out_of_memory"));
    case HAM_INV_PARAMETER:
      return (enif_make_atom(env, "inv_parameter"));
    case HAM_INV_FILE_HEADER:
      return (enif_make_atom(env, "inv_file_header"));
    case HAM_INV_FILE_VERSION:
      return (enif_make_atom(env, "inv_file_version"));
    case HAM_KEY_NOT_FOUND:
      return (g_atom_key_not_found);
    case HAM_DUPLICATE_KEY:
      return (g_atom_duplicate_key);
    case HAM_INTEGRITY_VIOLATED:
      return (enif_make_atom(env, "integrity_violated"));
    case HAM_INTERNAL_ERROR:
      return (enif_make_atom(env, "internal_error"));
    case HAM_WRITE_PROTECTED:
      return (enif_make_atom(env, "write_protected"));
    case HAM_BLOB_NOT_FOUND:
      return (enif_make_atom(env, "blob_not_found"));
    case HAM_IO_ERROR:
      return (enif_make_atom(env, "io_error"));
    case HAM_NOT_IMPLEMENTED:
      return (enif_make_atom(env, "not_implemented"));
    case HAM_FILE_NOT_FOUND:
      return (enif_make_atom(env, "file_not_found"));
    case HAM_WOULD_BLOCK:
      return (enif_make_atom(env, "would_block"));
    case HAM_NOT_READY:
      return (enif_make_atom(env, "not_ready"));
    case HAM_LIMITS_REACHED:
      return (enif_make_atom(env, "limits_reached"));
    case HAM_ALREADY_INITIALIZED:
      return (enif_make_atom(env, "already_initialized"));
    case HAM_NEED_RECOVERY:
      return (enif_make_atom(env, "need_recovery"));
    case HAM_CURSOR_STILL_OPEN:
      return (enif_make_atom(env, "cursor_still_open"));
    case HAM_FILTER_NOT_FOUND:
      return (enif_make_atom(env, "filter_not_found"));
    case HAM_TXN_CONFLICT:
      return (enif_make_atom(env, "txn_conflict"));
    case HAM_KEY_ERASED_IN_TXN:
      return (enif_make_atom(env, "key_erased_in_txn"));
    case HAM_TXN_STILL_OPEN:
      return (enif_make_atom(env, "txn_still_open"));
    case HAM_CURSOR_IS_NIL:
      return (enif_make_atom(env, "cursor_is_nil"));
    case HAM_DATABASE_NOT_FOUND:
      return (enif_make_atom(env, "database_not_found"));
    case HAM_DATABASE_ALREADY_EXISTS:
      return (enif_make_atom(env, "database_already_exists"));
    case HAM_DATABASE_ALREADY_OPEN:
      return (enif_make_atom(env, "database_already_open"));
    case HAM_ENVIRONMENT_ALREADY_OPEN:
      return (enif_make_atom(env, "environment_already_open"));
    case HAM_LOG_INV_FILE_HEADER:
      return (enif_make_atom(env, "log_inv_file_header"));
    case HAM_NETWORK_ERROR:
      return (enif_make_atom(env, "network_error"));
    default:
      return (g_atom_error);
  }
  return (0);
}

static int
get_parameters(ErlNifEnv *env, ERL_NIF_TERM term, ham_parameter_t *parameters,
            char *logdir_buf, char *aeskey_buf)
{
  unsigned i = 0;
  ERL_NIF_TERM cell;

  if (!enif_is_list(env, term))
    return (0);

  while (enif_get_list_cell(env, term, &cell, &term)) {
    int arity;
    char atom[128];
    const ERL_NIF_TERM *array;

    if (!enif_get_tuple(env, cell, &arity, &array) || arity != 2)
      return (0);
    if (enif_get_atom(env, array[0], &atom[0], sizeof(atom),
              ERL_NIF_LATIN1) <= 0)
      return (0);

    if (!strcmp(atom, "enable_journal_compression")) {
      parameters[i].name = HAM_PARAM_ENABLE_JOURNAL_COMPRESSION;
      if (!enif_get_uint64(env, array[1], &parameters[i].value))
        return (0);
      i++;
      continue;
    }
    if (!strcmp(atom, "enable_record_compression")) {
      parameters[i].name = HAM_PARAM_ENABLE_RECORD_COMPRESSION;
      if (!enif_get_uint64(env, array[1], &parameters[i].value))
        return (0);
      i++;
      continue;
    }
    if (!strcmp(atom, "cache_size")) {
      parameters[i].name = HAM_PARAM_CACHE_SIZE;
      if (!enif_get_uint64(env, array[1], &parameters[i].value))
        return (0);
      i++;
      continue;
    }
    if (!strcmp(atom, "page_size")) {
      parameters[i].name = HAM_PARAM_PAGE_SIZE;
      if (!enif_get_uint64(env, array[1], &parameters[i].value))
        return (0);
      i++;
      continue;
    }
    if (!strcmp(atom, "key_size")) {
      parameters[i].name = HAM_PARAM_KEY_SIZE;
      if (!enif_get_uint64(env, array[1], &parameters[i].value))
        return (0);
      i++;
      continue;
    }
    if (!strcmp(atom, "record_size")) {
      parameters[i].name = HAM_PARAM_RECORD_SIZE;
      if (!enif_get_uint64(env, array[1], &parameters[i].value))
        return (0);
      i++;
      continue;
    }
    if (!strcmp(atom, "max_databases")) {
      parameters[i].name = HAM_PARAM_MAX_DATABASES;
      if (!enif_get_uint64(env, array[1], &parameters[i].value))
        return (0);
      i++;
      continue;
    }
    if (!strcmp(atom, "key_type")) {
      parameters[i].name = HAM_PARAM_KEY_TYPE;
      if (!enif_get_uint64(env, array[1], &parameters[i].value))
        return (0);
      i++;
      continue;
    }
    if (!strcmp(atom, "log_directory")) {
      parameters[i].name = HAM_PARAM_LOG_DIRECTORY;
      if (!enif_get_string(env, array[1], logdir_buf, MAX_STRING,
              ERL_NIF_LATIN1) <= 0)
        return (0);
      parameters[i].value = *(ham_u64_t *)logdir_buf;
      i++;
      continue;
    }
    if (!strcmp(atom, "encryption_key")) {
      parameters[i].name = HAM_PARAM_ENCRYPTION_KEY;
      if (!enif_get_string(env, array[1], aeskey_buf, MAX_STRING,
              ERL_NIF_LATIN1) <= 0)
        return (0);
      parameters[i].value = *(ham_u64_t *)aeskey_buf;
      i++;
      continue;
    }
    if (!strcmp(atom, "network_timeout_sec")) {
      parameters[i].name = HAM_PARAM_NETWORK_TIMEOUT_SEC;
      if (!enif_get_uint64(env, array[1], &parameters[i].value))
        return (0);
      i++;
      continue;
    }

    // the following parameters are read-only; we do not need to
    // extract a value
    if (!strcmp(atom, "flags")) {
      parameters[i].name = HAM_PARAM_FLAGS;
      i++;
      continue;
    }
    if (!strcmp(atom, "filemode")) {
      parameters[i].name = HAM_PARAM_FILEMODE;
      i++;
      continue;
    }
    if (!strcmp(atom, "filename")) {
      parameters[i].name = HAM_PARAM_FILENAME;
      i++;
      continue;
    }
    if (!strcmp(atom, "database_name")) {
      parameters[i].name = HAM_PARAM_DATABASE_NAME;
      i++;
      continue;
    }
    if (!strcmp(atom, "max_keys_per_page")) {
      parameters[i].name = HAM_PARAM_MAX_KEYS_PER_PAGE;
      i++;
      continue;
    }

    // still here? that's an error
    return (0);
  }

  return (1);
}

ERL_NIF_TERM
ham_nifs_strerror(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_status_t st;

  if (argc != 1)
    return (enif_make_badarg(env));
  if (!enif_get_int(env, argv[0], &st))
    return (enif_make_badarg(env));

  return (enif_make_string(env, ham_strerror(st), ERL_NIF_LATIN1));
}

ERL_NIF_TERM
ham_nifs_get_license(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  if (argc != 0)
    return (enif_make_badarg(env));

  const char *licensee = 0;
  const char *product = 0;
  ham_get_license(&licensee, &product);
  if (!licensee)
    licensee = "";

  return (enif_make_tuple2(env,
        enif_make_string(env, licensee, ERL_NIF_LATIN1),
        enif_make_string(env, product, ERL_NIF_LATIN1)));
}

ERL_NIF_TERM
ham_nifs_env_create(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_env_t *henv;
  ham_u32_t flags = 0;
  ham_u32_t mode = 0;
  char filename[MAX_STRING];
  ham_parameter_t parameters[MAX_PARAMETERS] = {{0, 0}};
  char logdir_buf[MAX_STRING];
  char aesdir_buf[MAX_STRING];

  if (argc != 4)
    return (enif_make_badarg(env));
  if (enif_get_string(env, argv[0], &filename[0], sizeof(filename),
              ERL_NIF_LATIN1) <= 0)
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[1], &flags))
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[2], &mode))
    return (enif_make_badarg(env));
  if (!get_parameters(env, argv[3], &parameters[0],
              &logdir_buf[0], &aesdir_buf[0]))
    return (enif_make_badarg(env));

  ham_status_t st = ham_env_create(&henv, filename, flags, mode, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  env_wrapper *ewrapper = (env_wrapper *)enif_alloc_resource_compat(env,
                                g_ham_env_resource, sizeof(*ewrapper));
  ewrapper->env = henv;
  ewrapper->is_closed = false;
  ERL_NIF_TERM result = enif_make_resource(env, ewrapper);
  enif_release_resource_compat(env, ewrapper);

  return (enif_make_tuple2(env, g_atom_ok, result));
}

ERL_NIF_TERM
ham_nifs_env_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_env_t *henv;
  ham_u32_t flags;
  char filename[MAX_STRING];
  ham_parameter_t parameters[MAX_PARAMETERS] = {{0, 0}};
  char logdir_buf[MAX_STRING];
  char aesdir_buf[MAX_STRING];

  if (argc != 3)
    return (enif_make_badarg(env));
  if (enif_get_string(env, argv[0], &filename[0], sizeof(filename),
              ERL_NIF_LATIN1) <= 0)
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[1], &flags))
    return (enif_make_badarg(env));
  if (!get_parameters(env, argv[2], &parameters[0],
              &logdir_buf[0], &aesdir_buf[0]))
    return (enif_make_badarg(env));

  ham_status_t st = ham_env_open(&henv, filename, flags, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  env_wrapper *ewrapper = (env_wrapper *)enif_alloc_resource_compat(env,
                                g_ham_env_resource, sizeof(*ewrapper));
  ewrapper->env = henv;
  ewrapper->is_closed = false;
  ERL_NIF_TERM result = enif_make_resource(env, ewrapper);
  enif_release_resource_compat(env, ewrapper);

  return (enif_make_tuple2(env, g_atom_ok, result));
}

ERL_NIF_TERM
ham_nifs_env_create_db(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_db_t *hdb;
  ham_u32_t dbname;
  ham_u32_t flags;
  ham_parameter_t parameters[MAX_PARAMETERS] = {{0, 0}};
  char logdir_buf[MAX_STRING];
  char aesdir_buf[MAX_STRING];
  env_wrapper *ewrapper;

  if (argc != 4)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_env_resource, (void **)&ewrapper)
          || ewrapper->is_closed)
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[1], &dbname))
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[2], &flags))
    return (enif_make_badarg(env));
  if (!get_parameters(env, argv[3], &parameters[0],
              &logdir_buf[0], &aesdir_buf[0]))
    return (enif_make_badarg(env));

  ham_status_t st = ham_env_create_db(ewrapper->env, &hdb, dbname, flags, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  db_wrapper *dbwrapper = (db_wrapper *)enif_alloc_resource_compat(env,
                                g_ham_db_resource, sizeof(*dbwrapper));
  dbwrapper->db = hdb;
  dbwrapper->is_closed = false;
  ERL_NIF_TERM result = enif_make_resource(env, dbwrapper);
  enif_release_resource_compat(env, dbwrapper);

  return (enif_make_tuple2(env, g_atom_ok, result));
}

ERL_NIF_TERM
ham_nifs_env_open_db(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_db_t *hdb;
  ham_u32_t dbname;
  ham_u32_t flags;
  ham_parameter_t parameters[MAX_PARAMETERS] = {{0, 0}};
  char logdir_buf[MAX_STRING];
  char aesdir_buf[MAX_STRING];
  env_wrapper *ewrapper;

  if (argc != 4)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_env_resource, (void **)&ewrapper)
          || ewrapper->is_closed)
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[1], &dbname))
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[2], &flags))
    return (enif_make_badarg(env));
  if (!get_parameters(env, argv[3], &parameters[0],
              &logdir_buf[0], &aesdir_buf[0]))
    return (enif_make_badarg(env));

  ham_status_t st = ham_env_open_db(ewrapper->env, &hdb, dbname, flags, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  db_wrapper *dbwrapper = (db_wrapper *)enif_alloc_resource_compat(env,
                                g_ham_db_resource, sizeof(*dbwrapper));
  dbwrapper->db = hdb;
  dbwrapper->is_closed = false;
  ERL_NIF_TERM result = enif_make_resource(env, dbwrapper);
  enif_release_resource_compat(env, dbwrapper);

  return (enif_make_tuple2(env, g_atom_ok, result));
}

ERL_NIF_TERM
ham_nifs_env_erase_db(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_u32_t dbname;
  env_wrapper *ewrapper;

  if (argc != 2)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_env_resource, (void **)&ewrapper)
          || ewrapper->is_closed)
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[1], &dbname))
    return (enif_make_badarg(env));

  ham_status_t st = ham_env_erase_db(ewrapper->env, dbname, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (g_atom_ok);
}

ERL_NIF_TERM
ham_nifs_env_rename_db(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_u32_t oldname;
  ham_u32_t newname;
  env_wrapper *ewrapper;

  if (argc != 3)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_env_resource, (void **)&ewrapper)
          || ewrapper->is_closed)
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[1], &oldname))
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[2], &newname))
    return (enif_make_badarg(env));

  ham_status_t st = ham_env_rename_db(ewrapper->env, oldname, newname, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (g_atom_ok);
}

ERL_NIF_TERM
ham_nifs_db_insert(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_key_t key = {0};
  ham_record_t rec = {0};
  ham_u32_t flags;
  ErlNifBinary binkey;
  ErlNifBinary binrec;
  db_wrapper *dwrapper;
  txn_wrapper *twrapper;

  if (argc != 5)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_db_resource, (void **)&dwrapper)
          || dwrapper->is_closed)
    return (enif_make_badarg(env));
  // arg[1] is the Transaction!
  if (!enif_get_resource(env, argv[1], g_ham_txn_resource, (void **)&twrapper))
    twrapper = 0;
  if (twrapper != 0 && twrapper->is_closed)
    return (enif_make_badarg(env));
  if (!enif_inspect_binary(env, argv[2], &binkey))
    return (enif_make_badarg(env));
  if (!enif_inspect_binary(env, argv[3], &binrec))
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[4], &flags))
    return (enif_make_badarg(env));

  key.size = binkey.size;
  key.data = binkey.size ? binkey.data : 0;
  rec.size = binrec.size;
  rec.data = binrec.size ? binrec.data : 0;

  ham_status_t st = ham_db_insert(dwrapper->db, twrapper ? twrapper->txn : 0,
                                &key, &rec, flags);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (g_atom_ok);
}

ERL_NIF_TERM
ham_nifs_db_erase(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_key_t key = {0};
  ErlNifBinary binkey;
  db_wrapper *dwrapper;
  txn_wrapper *twrapper;

  if (argc != 3)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_db_resource, (void **)&dwrapper)
          || dwrapper->is_closed)
    return (enif_make_badarg(env));
  // arg[1] is the Transaction!
  if (!enif_get_resource(env, argv[1], g_ham_txn_resource, (void **)&twrapper))
    twrapper = 0;
  if (twrapper != 0 && twrapper->is_closed)
    return (enif_make_badarg(env));
  if (!enif_inspect_binary(env, argv[2], &binkey))
    return (enif_make_badarg(env));

  key.data = binkey.data;
  key.size = binkey.size;

  ham_status_t st = ham_db_erase(dwrapper->db, twrapper ? twrapper->txn : 0,
                        &key, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (g_atom_ok);
}

ERL_NIF_TERM
ham_nifs_db_find(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_key_t key = {0};
  ham_record_t rec = {0};
  ErlNifBinary binkey;
  ErlNifBinary binrec;
  db_wrapper *dwrapper;
  txn_wrapper *twrapper;

  if (argc != 3)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_db_resource, (void **)&dwrapper)
          || dwrapper->is_closed)
    return (enif_make_badarg(env));
  // argv[1] is the Transaction!
  if (!enif_get_resource(env, argv[1], g_ham_txn_resource, (void **)&twrapper))
    twrapper = 0;
  if (twrapper != 0 && twrapper->is_closed)
    return (enif_make_badarg(env));
  if (!enif_inspect_binary(env, argv[2], &binkey))
    return (enif_make_badarg(env));

  key.data = binkey.data;
  key.size = binkey.size;

  ham_status_t st = ham_db_find(dwrapper->db, twrapper ? twrapper->txn : 0,
                                &key, &rec, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  if (!enif_alloc_binary(rec.size, &binrec))
    return (enif_make_tuple2(env, g_atom_error,
                status_to_atom(env, HAM_OUT_OF_MEMORY)));

  memcpy(binrec.data, rec.data, rec.size);
  binrec.size = rec.size;

  return (enif_make_tuple2(env, g_atom_ok, enif_make_binary(env, &binrec)));
}

ERL_NIF_TERM
ham_nifs_txn_begin(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  env_wrapper *ewrapper;
  ham_u32_t flags;

  if (argc != 2)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_env_resource, (void **)&ewrapper)
          || ewrapper->is_closed)
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[1], &flags))
    return (enif_make_badarg(env));

  ham_txn_t *txn;
  ham_status_t st = ham_txn_begin(&txn, ewrapper->env, 0, 0, flags);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  txn_wrapper *twrapper = (txn_wrapper *)enif_alloc_resource_compat(env,
                                g_ham_txn_resource, sizeof(*twrapper));
  twrapper->txn = txn;
  twrapper->is_closed = false;
  ERL_NIF_TERM result = enif_make_resource(env, twrapper);
  enif_release_resource_compat(env, twrapper);

  return (enif_make_tuple2(env, g_atom_ok, result));
}

ERL_NIF_TERM
ham_nifs_txn_abort(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  txn_wrapper *twrapper;

  if (argc != 1)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_txn_resource, (void **)&twrapper)
          || twrapper->is_closed)
    return (enif_make_badarg(env));

  ham_status_t st = ham_txn_abort(twrapper->txn, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  twrapper->is_closed = true;
  return (g_atom_ok);
}

ERL_NIF_TERM
ham_nifs_txn_commit(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  txn_wrapper *twrapper;

  if (argc != 1)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_txn_resource, (void **)&twrapper)
          || twrapper->is_closed)
    return (enif_make_badarg(env));

  ham_status_t st = ham_txn_commit(twrapper->txn, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  twrapper->is_closed = true;
  return (g_atom_ok);
}

ERL_NIF_TERM
ham_nifs_db_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  db_wrapper *dwrapper;

  if (argc != 1)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_db_resource, (void **)&dwrapper)
          || dwrapper->is_closed)
    return (enif_make_badarg(env));

  ham_status_t st = ham_db_close(dwrapper->db, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  dwrapper->is_closed = true;
  return (g_atom_ok);
}

ERL_NIF_TERM
ham_nifs_env_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  env_wrapper *ewrapper;

  if (argc != 1)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_env_resource, (void **)&ewrapper)
          || ewrapper->is_closed)
    return (enif_make_badarg(env));

  ham_status_t st = ham_env_close(ewrapper->env, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  ewrapper->is_closed = true;
  return (g_atom_ok);
}

ERL_NIF_TERM
ham_nifs_cursor_create(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  db_wrapper *dwrapper;
  txn_wrapper *twrapper;

  if (argc != 2)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_db_resource, (void **)&dwrapper)
          || dwrapper->is_closed)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[1], g_ham_txn_resource, (void **)&twrapper))
    twrapper = 0;
  if (twrapper && twrapper->is_closed)
    return (enif_make_badarg(env));

  ham_cursor_t *cursor;
  ham_status_t st = ham_cursor_create(&cursor, dwrapper->db,
                                    twrapper ? twrapper ->txn : 0, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  cursor_wrapper *cwrapper = (cursor_wrapper *)enif_alloc_resource_compat(env,
                                g_ham_cursor_resource, sizeof(*cwrapper));
  cwrapper->cursor = cursor;
  cwrapper->is_closed = false;
  ERL_NIF_TERM result = enif_make_resource(env, cwrapper);
  enif_release_resource_compat(env, cwrapper);

  return (enif_make_tuple2(env, g_atom_ok, result));
}

ERL_NIF_TERM
ham_nifs_cursor_clone(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  cursor_wrapper *cwrapper;

  if (argc != 1)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_cursor_resource,
              (void **)&cwrapper)
          || cwrapper->is_closed)
    return (enif_make_badarg(env));

  ham_cursor_t *clone;
  ham_status_t st = ham_cursor_clone(cwrapper->cursor, &clone);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  cursor_wrapper *c2wrapper = (cursor_wrapper *)enif_alloc_resource_compat(env,
                                g_ham_cursor_resource, sizeof(*c2wrapper));
  c2wrapper->cursor = clone;
  c2wrapper->is_closed = false;
  ERL_NIF_TERM result = enif_make_resource(env, c2wrapper);
  enif_release_resource_compat(env, c2wrapper);

  return (enif_make_tuple2(env, g_atom_ok, result));
}

ERL_NIF_TERM
ham_nifs_cursor_move(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  cursor_wrapper *cwrapper;
  ham_u32_t flags;

  if (argc != 2)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_cursor_resource,
              (void **)&cwrapper)
          || cwrapper->is_closed)
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[1], &flags))
    return (enif_make_badarg(env));

  ham_key_t key = {0};
  ham_record_t rec = {0};
  ham_status_t st = ham_cursor_move(cwrapper->cursor, &key, &rec, flags);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  ErlNifBinary binkey;
  if (!enif_alloc_binary(key.size, &binkey))
    return (enif_make_tuple2(env, g_atom_error,
                status_to_atom(env, HAM_OUT_OF_MEMORY)));
  memcpy(binkey.data, key.data, key.size);
  binkey.size = key.size;

  ErlNifBinary binrec;
  if (!enif_alloc_binary(rec.size, &binrec))
    return (enif_make_tuple2(env, g_atom_error,
                status_to_atom(env, HAM_OUT_OF_MEMORY)));
  memcpy(binrec.data, rec.data, rec.size);
  binrec.size = rec.size;

  return (enif_make_tuple3(env, g_atom_ok,
              enif_make_binary(env, &binkey),
              enif_make_binary(env, &binrec)));
}

ERL_NIF_TERM
ham_nifs_cursor_overwrite(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  cursor_wrapper *cwrapper;
  ErlNifBinary binrec;

  if (argc != 2)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_cursor_resource,
              (void **)&cwrapper)
          || cwrapper->is_closed)
    return (enif_make_badarg(env));
  if (!enif_inspect_binary(env, argv[1], &binrec))
    return (enif_make_badarg(env));

  ham_record_t rec = {0};
  rec.data = binrec.data;
  rec.size = binrec.size;

  ham_status_t st = ham_cursor_overwrite(cwrapper->cursor, &rec, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (g_atom_ok);
}

ERL_NIF_TERM
ham_nifs_cursor_find(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  cursor_wrapper *cwrapper;
  ErlNifBinary binkey;

  if (argc != 2)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_cursor_resource,
              (void **)&cwrapper)
          || cwrapper->is_closed)
    return (enif_make_badarg(env));
  if (!enif_inspect_binary(env, argv[1], &binkey))
    return (enif_make_badarg(env));

  ham_record_t rec = {0};
  ham_key_t key = {0};
  key.data = binkey.data;
  key.size = binkey.size;

  ham_status_t st = ham_cursor_find(cwrapper->cursor, &key, &rec, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  ErlNifBinary binrec;
  if (!enif_alloc_binary(rec.size, &binrec))
    return (enif_make_tuple2(env, g_atom_error,
                status_to_atom(env, HAM_OUT_OF_MEMORY)));
  memcpy(binrec.data, rec.data, rec.size);
  binrec.size = rec.size;

  return (enif_make_tuple2(env, g_atom_ok, enif_make_binary(env, &binrec)));
}

ERL_NIF_TERM
ham_nifs_cursor_insert(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  cursor_wrapper *cwrapper;
  ErlNifBinary binkey;
  ErlNifBinary binrec;
  ham_u32_t flags;

  if (argc != 4)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_cursor_resource,
              (void **)&cwrapper)
          || cwrapper->is_closed)
    return (enif_make_badarg(env));
  if (!enif_inspect_binary(env, argv[1], &binkey))
    return (enif_make_badarg(env));
  if (!enif_inspect_binary(env, argv[2], &binrec))
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[3], &flags))
    return (enif_make_badarg(env));

  ham_key_t key = {0};
  key.data = binkey.data;
  key.size = binkey.size;
  ham_record_t rec = {0};
  rec.data = binrec.data;
  rec.size = binrec.size;

  ham_status_t st = ham_cursor_insert(cwrapper->cursor, &key, &rec, flags);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (g_atom_ok);
}

ERL_NIF_TERM
ham_nifs_cursor_erase(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  cursor_wrapper *cwrapper;

  if (argc != 1)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_cursor_resource,
              (void **)&cwrapper)
          || cwrapper->is_closed)
    return (enif_make_badarg(env));

  ham_status_t st = ham_cursor_erase(cwrapper->cursor, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (g_atom_ok);
}

ERL_NIF_TERM
ham_nifs_cursor_get_duplicate_count(ErlNifEnv *env, int argc,
        const ERL_NIF_TERM argv[])
{
  cursor_wrapper *cwrapper;

  if (argc != 1)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_cursor_resource,
              (void **)&cwrapper)
          || cwrapper->is_closed)
    return (enif_make_badarg(env));

  ham_u32_t count;
  ham_status_t st = ham_cursor_get_duplicate_count(cwrapper->cursor, &count, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (enif_make_tuple2(env, g_atom_ok, enif_make_int(env, (int)count)));
}

ERL_NIF_TERM
ham_nifs_cursor_get_record_size(ErlNifEnv *env, int argc,
        const ERL_NIF_TERM argv[])
{
  cursor_wrapper *cwrapper;

  if (argc != 1)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_cursor_resource,
              (void **)&cwrapper)
          || cwrapper->is_closed)
    return (enif_make_badarg(env));

  ham_u64_t size;
  ham_status_t st = ham_cursor_get_record_size(cwrapper->cursor, &size);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (enif_make_tuple2(env, g_atom_ok, enif_make_int64(env, (int)size)));
}

ERL_NIF_TERM
ham_nifs_cursor_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  cursor_wrapper *cwrapper;

  if (argc != 1)
    return (enif_make_badarg(env));
  if (!enif_get_resource(env, argv[0], g_ham_cursor_resource,
              (void **)&cwrapper)
          || cwrapper->is_closed)
    return (enif_make_badarg(env));

  ham_status_t st = ham_cursor_close(cwrapper->cursor);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  cwrapper->is_closed = true;
  return (g_atom_ok);
}

static void
env_resource_cleanup(ErlNifEnv *env, void *arg)
{
  env_wrapper *ewrapper = (env_wrapper *)arg;
  if (!ewrapper->is_closed)
    (void)ham_env_close(ewrapper->env, 0);
  ewrapper->is_closed = true;
}

static void
db_resource_cleanup(ErlNifEnv *env, void *arg)
{
  db_wrapper *dwrapper = (db_wrapper *)arg;
  if (!dwrapper->is_closed)
    (void)ham_db_close(dwrapper->db, 0);
  dwrapper->is_closed = true;
}

static void
txn_resource_cleanup(ErlNifEnv *env, void *arg)
{
  txn_wrapper *twrapper = (txn_wrapper *)arg;
  if (!twrapper->is_closed)
    (void)ham_txn_abort(twrapper->txn, 0);
  twrapper->is_closed = true;
}

static void
cursor_resource_cleanup(ErlNifEnv *env, void *arg)
{
  cursor_wrapper *cwrapper = (cursor_wrapper *)arg;
  if (!cwrapper->is_closed)
    (void)ham_cursor_close(cwrapper->cursor);
  cwrapper->is_closed = true;
}

static int
on_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
  g_atom_ok = enif_make_atom(env, "ok");
  g_atom_error = enif_make_atom(env, "error");
  g_atom_key_not_found = enif_make_atom(env, "key_not_found");
  g_atom_duplicate_key = enif_make_atom(env, "duplicate_key");

  g_ham_env_resource = enif_open_resource_type(env, NULL, "ham_env_resource",
                            &env_resource_cleanup,
                            (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER),
                            0);
  g_ham_db_resource = enif_open_resource_type(env, NULL, "ham_db_resource",
                            &db_resource_cleanup,
                            (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER),
                            0);
  g_ham_txn_resource = enif_open_resource_type(env, NULL, "ham_txn_resource",
                            &txn_resource_cleanup,
                            (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER),
                            0);
  g_ham_cursor_resource = enif_open_resource_type(env, NULL, "ham_cursor_resource",
                            &cursor_resource_cleanup,
                            (ErlNifResourceFlags)(ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER),
                            0);
  return (0);
}

extern "C" {

static ErlNifFunc ham_nif_funcs[] =
{
  {"strerror", 1, ham_nifs_strerror},
  {"get_license", 0, ham_nifs_get_license},
  {"env_create", 4, ham_nifs_env_create},
  {"env_open", 3, ham_nifs_env_open},
  {"env_create_db", 4, ham_nifs_env_create_db},
  {"env_open_db", 4, ham_nifs_env_open_db},
  {"env_rename_db", 3, ham_nifs_env_rename_db},
  {"env_erase_db", 2, ham_nifs_env_erase_db},
  {"db_insert", 5, ham_nifs_db_insert},
  {"db_erase", 3, ham_nifs_db_erase},
  {"db_find", 3, ham_nifs_db_find},
  {"db_close", 1, ham_nifs_db_close},
  {"txn_begin", 2, ham_nifs_txn_begin},
  {"txn_abort", 1, ham_nifs_txn_abort},
  {"txn_commit", 1, ham_nifs_txn_commit},
  {"env_close", 1, ham_nifs_env_close},
  {"cursor_create", 2, ham_nifs_cursor_create},
  {"cursor_clone", 1, ham_nifs_cursor_clone},
  {"cursor_move", 2, ham_nifs_cursor_move},
  {"cursor_overwrite", 2, ham_nifs_cursor_overwrite},
  {"cursor_find", 2, ham_nifs_cursor_find},
  {"cursor_insert", 4, ham_nifs_cursor_insert},
  {"cursor_erase", 1, ham_nifs_cursor_erase},
  {"cursor_get_duplicate_count", 1, ham_nifs_cursor_get_duplicate_count},
  {"cursor_get_record_size", 1, ham_nifs_cursor_get_record_size},
  {"cursor_close", 1, ham_nifs_cursor_close},
};

ERL_NIF_INIT(ham_nifs, ham_nif_funcs, on_load, NULL, NULL, NULL);

}; // extern "C"
