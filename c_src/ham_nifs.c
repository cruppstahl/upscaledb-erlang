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

#define MAX_PARAMETERS 64
#define MAX_STRING 2048

static ERL_NIF_TERM
pointer_to_term(ErlNifEnv *env, void *ptr)
{
  return (enif_make_int64(env, (unsigned long long)ptr));
}

static void *
term_to_pointer(ErlNifEnv *env, ERL_NIF_TERM term)
{
  void *p = 0;
  enif_get_long(env, term, (long int *)&p);
  return (p);
}

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
ham_nifs_env_create(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_env_t *henv;
  ham_u32_t flags = 0;
  ham_u32_t mode = 0;
  ham_status_t st;
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

  st = ham_env_create(&henv, filename, flags, mode, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (enif_make_tuple2(env, g_atom_ok, pointer_to_term(env, henv)));
}

ERL_NIF_TERM
ham_nifs_env_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_env_t *henv;
  ham_u32_t flags;
  ham_status_t st;
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

  st = ham_env_open(&henv, filename, flags, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (enif_make_tuple2(env, g_atom_ok, pointer_to_term(env, henv)));
}

ERL_NIF_TERM
ham_nifs_env_create_db(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_env_t *henv;
  ham_db_t *hdb;
  ham_status_t st;
  ham_u32_t dbname;
  ham_u32_t flags;
  ham_parameter_t parameters[MAX_PARAMETERS] = {{0, 0}};
  char logdir_buf[MAX_STRING];
  char aesdir_buf[MAX_STRING];

  if (argc != 4)
    return (enif_make_badarg(env));
  if (!(henv = (ham_env_t *)term_to_pointer(env, argv[0])))
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[1], &dbname))
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[2], &flags))
    return (enif_make_badarg(env));
  if (!get_parameters(env, argv[3], &parameters[0],
              &logdir_buf[0], &aesdir_buf[0]))
    return (enif_make_badarg(env));

  st = ham_env_create_db(henv, &hdb, dbname, flags, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (enif_make_tuple2(env, g_atom_ok, pointer_to_term(env, hdb)));
}

ERL_NIF_TERM
ham_nifs_env_open_db(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_env_t *henv;
  ham_db_t *hdb;
  ham_status_t st;
  ham_u32_t dbname;
  ham_u32_t flags;
  ham_parameter_t parameters[MAX_PARAMETERS] = {{0, 0}};
  char logdir_buf[MAX_STRING];
  char aesdir_buf[MAX_STRING];

  if (argc != 4)
    return (enif_make_badarg(env));
  if (!(henv = (ham_env_t *)term_to_pointer(env, argv[0])))
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[1], &dbname))
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[2], &flags))
    return (enif_make_badarg(env));
  if (!get_parameters(env, argv[3], &parameters[0],
              &logdir_buf[0], &aesdir_buf[0]))
    return (enif_make_badarg(env));

  st = ham_env_open_db(henv, &hdb, dbname, flags, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (enif_make_tuple2(env, g_atom_ok, pointer_to_term(env, hdb)));
}

// ok
ERL_NIF_TERM
ham_nifs_env_erase_db(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_env_t *henv;
  ham_status_t st;
  ham_u32_t dbname;

  if (argc != 2)
    return (enif_make_badarg(env));
  if (!(henv = (ham_env_t *)term_to_pointer(env, argv[0])))
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[1], &dbname))
    return (enif_make_badarg(env));

  st = ham_env_erase_db(henv, dbname, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (g_atom_ok);
}

// ok
ERL_NIF_TERM
ham_nifs_env_rename_db(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_env_t *henv;
  ham_status_t st;
  ham_u32_t oldname;
  ham_u32_t newname;

  if (argc != 3)
    return (enif_make_badarg(env));
  if (!(henv = (ham_env_t *)term_to_pointer(env, argv[0])))
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[1], &oldname))
    return (enif_make_badarg(env));
  if (!enif_get_uint(env, argv[2], &newname))
    return (enif_make_badarg(env));

  st = ham_env_rename_db(henv, oldname, newname, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (g_atom_ok);
}

// ok
ERL_NIF_TERM
ham_nifs_db_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_db_t *hdb;
  ham_status_t st;

  if (argc != 1)
    return (enif_make_badarg(env));
  if (!(hdb = (ham_db_t *)term_to_pointer(env, argv[0])))
    return (enif_make_badarg(env));

  st = ham_db_close(hdb, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (g_atom_ok);
}

// ok
ERL_NIF_TERM
ham_nifs_env_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  ham_env_t *henv;
  ham_status_t st;

  if (argc != 1)
    return (enif_make_badarg(env));
  if (!(henv = (ham_env_t *)term_to_pointer(env, argv[0])))
    return (enif_make_badarg(env));

  st = ham_env_close(henv, 0);
  if (st)
    return (enif_make_tuple2(env, g_atom_error, status_to_atom(env, st)));

  return (g_atom_ok);
}

static int
on_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
  g_atom_ok = enif_make_atom(env, "ok");
  g_atom_error = enif_make_atom(env, "error");
  g_atom_key_not_found = enif_make_atom(env, "key_not_found");
  g_atom_duplicate_key = enif_make_atom(env, "duplicate_key");
  return (0);
}

static ErlNifFunc ham_nif_funcs[] =
{
  {"strerror", 1, ham_nifs_strerror},
  {"env_create", 4, ham_nifs_env_create},
  {"env_open", 3, ham_nifs_env_open},
  {"env_create_db", 4, ham_nifs_env_create_db},
  {"env_open_db", 4, ham_nifs_env_open_db},
  {"env_rename_db", 3, ham_nifs_env_rename_db},
  {"env_erase_db", 2, ham_nifs_env_erase_db},
  {"db_close", 1, ham_nifs_db_close},
  {"env_close", 1, ham_nifs_env_close}
};

ERL_NIF_INIT(ham_nifs, ham_nif_funcs, on_load, NULL, NULL, NULL);

