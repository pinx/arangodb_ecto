defmodule ArangoDB.Ecto do
  @moduledoc false

  require Logger

  # Inherit all behaviour from Ecto.Adapters.SQL
  use Ecto.Adapters.SQL, :arango_db

  # And provide a custom storage implementation
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Structure

  ## Storage API

  @impl true
  def storage_up(opts) do
    database =
      Keyword.fetch!(opts, :database) || raise ":database is nil in repository configuration"

    response = Arango.Database.create(name: database) |> Arango.request()

    case response do
      {:ok, _} -> :ok
      {:error, %{"code" => 409}} -> {:error, :already_up}
      {:error, _} -> response
    end
  end

  @impl true
  def storage_down(opts) do
    database =
      Keyword.fetch!(opts, :database) || raise ":database is nil in repository configuration"

    response = Arango.Database.drop(database) |> Arango.request()

    case response do
      {:ok, _} -> :ok
      {:error, %{"code" => 404}} -> {:error, :already_down}
      {:error, _} -> response
    end
  end

  @impl true
  def supports_ddl_transaction? do
    false
  end

  @impl true
  def structure_dump(default, config) do
    table = config[:migration_source] || "schema_migrations"

    with {:ok, versions} <- select_versions(table, config),
         {:ok, path} <- pg_dump(default, config),
         do: append_versions(table, versions, path)
  end

  defp select_versions(table, config) do
    case run_query(~s[SELECT version FROM public."#{table}" ORDER BY version], config) do
      {:ok, %{rows: rows}} ->
        {:ok, Enum.map(rows, &hd/1)}

      {
        :error,
        %{
          postgres: %{
            code: :undefined_table
          }
        }
      } ->
        {:ok, []}

      {:error, _} = error ->
        error
    end
  end

  defp pg_dump(default, config) do
    path = config[:dump_path] || Path.join(default, "structure.sql")
    File.mkdir_p!(Path.dirname(path))

    case run_with_cmd(
           "arangodump",
           config,
           [
             "--output-directory #{path}",
             "--dump-data false",
             "--server.database #{config[:database]}"
           ]
         ) do
      {_output, 0} ->
        {:ok, path}

      {output, _} ->
        {:error, output}
    end
  end

  defp append_versions(_table, [], path) do
    {:ok, path}
  end

  defp append_versions(table, versions, path) do
    sql = "FOR v IN #{versions} INSERT {'version': v} IN #{table}"

    File.open!(
      path,
      [:append],
      fn file ->
        IO.write(file, sql)
      end
    )

    {:ok, path}
  end

  @impl true
  def structure_load(default, config) do
    path = config[:dump_path] || Path.join(default, "structure.sql")

    args = [
      "--input-directory #{path}",
      "--server-database #{config[:database]}"
    ]

    case run_with_cmd("arangorestore", config, args) do
      {_output, 0} -> {:ok, path}
      {output, _} -> {:error, output}
    end
  end

  ## Helpers

  defp run_query(sql, opts) do
    Logger.debug(sql)
    # {:ok, _} = Application.ensure_all_started(:postgrex)

    # opts =
    #   opts
    #   |> Keyword.drop([:name, :log, :pool, :pool_size])
    #   |> Keyword.put(:backoff_type, :stop)
    #   |> Keyword.put(:max_restarts, 0)

    # {:ok, pid} = Task.Supervisor.start_link

    # task = Task.Supervisor.async_nolink(pid, fn ->
    #   {:ok, conn} = Postgrex.start_link(opts)

    #   value = Postgrex.query(conn, sql, [], opts)
    #   GenServer.stop(conn)
    #   value
    # end)

    # timeout = Keyword.get(opts, :timeout, 15_000)

    # case Task.yield(task, timeout) || Task.shutdown(task) do
    #   {:ok, {:ok, result}} ->
    #     {:ok, result}
    #   {:ok, {:error, error}} ->
    #     {:error, error}
    #   {:exit, {%{__struct__: struct} = error, _}}
    #       when struct in [Postgrex.Error, DBConnection.Error] ->
    #     {:error, error}
    #   {:exit, reason}  ->
    #     {:error, RuntimeError.exception(Exception.format_exit(reason))}
    #   nil ->
    #     {:error, RuntimeError.exception("command timed out")}
    # end
  end

  defp run_with_cmd(cmd, opts, opt_args) do
    unless System.find_executable(cmd) do
      raise "could not find executable `#{cmd}` in path, " <>
              "please guarantee it is available before running ecto commands"
    end

    env = [{"PGCONNECT_TIMEOUT", "10"}]

    env =
      if password = opts[:password] do
        [{"PGPASSWORD", password} | env]
      else
        env
      end

    args = []
    args = if username = opts[:username], do: ["--server.username", username | args], else: args
    port = if port = opts[:port], do: ":" <> to_string(port), else: ""
    host = opts[:hostname] <> port || System.get_env("PGHOST") || "localhost"
    args = ["--server.endpoint", host | args]
    args = args ++ opt_args
    System.cmd(cmd, args, env: env, stderr_to_stdout: true)
  end
end
