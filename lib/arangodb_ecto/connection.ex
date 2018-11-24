defmodule ArangoDB.Ecto.Connection do
  @moduledoc false

  @behaviour Ecto.Adapters.SQL.Connection

  require Logger

  ## Connection

  @impl true
  def child_spec(opts) do
    IO.inspect(opts, label: "Startup options")
    {:ok, self()}
  end

  ## Query

  @impl true
  def prepare_execute(conn, name, sql, params, opts) do
    Logger.debug(sql, label: "prepare_execute")
  end

  @impl true
  def query(conn, sql, params, opts) do
    Logger.debug(sql, label: "query")
  end

  @impl true
  def execute(conn, %{ref: ref} = query, params, opts) do
    Logger.debug(query, label: "execute")
    # case Postgrex.execute(conn, query, params, opts) do
    #   {:ok, %{ref: ^ref}, result} ->
    #     {:ok, result}

    #   {:ok, _, _} = ok ->
    #     ok

    #   {:error, %Postgrex.QueryError{} = err} ->
    #     {:reset, err}

    #   {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}} = err} ->
    #     {:reset, err}

    #   {:error, _} = error ->
    #     error
    # end
  end

  @impl true
  def stream(conn, sql, params, opts) do
    Logger.debug(sql, label: "stream")
    # Postgrex.stream(conn, sql, params, opts)
  end

  @impl true
  def to_constraints(_),
    do: []

  @impl true
  def all(query) do
    sources = create_names(query)

    from = from(query, sources)
    join = join(query, sources)
    where = where(query, sources)
    order_by = order_by(query, sources)
    offset_and_limit = offset_and_limit(query, sources)
    select = select(query, sources)

    [from, join, where, order_by, offset_and_limit, select]
  end

  @impl true
  def update_all(query) do
    sources = create_names(query)

    from = from(query, sources)
    join = join(query, sources)
    where = where(query, sources)
    order_by = order_by(query, sources)
    offset_and_limit = offset_and_limit(query, sources)
    update = update(query, sources)
    return = returning("NEW", query, sources)

    [from, join, where, order_by, offset_and_limit, update, return]
  end

  @impl true
  def delete_all(query) do
    sources = create_names(query)

    from = from(query, sources)
    join = join(query, sources)
    where = where(query, sources)
    order_by = order_by(query, sources)
    offset_and_limit = offset_and_limit(query, sources)
    remove = remove(query, sources)
    return = returning("OLD", query, sources)

    [from, join, where, order_by, offset_and_limit, remove, return]
  end

  @impl true
  def insert(prefix, table, header, rows, on_conflict, returning) do
    ["FOR doc IN ", insert_all(rows), " INSERT doc IN ", quote_table(prefix, table)]
  end

  defp insert_all(rows) do
    intersperse_map(rows, ?,, fn row ->
      [?(, intersperse_map(row, ?,, &insert_all_value/1), ?)]
    end)
  end

  defp insert_all_value(nil), do: "DEFAULT"
  defp insert_all_value({%Ecto.Query{} = query, _params_counter}), do: [?(, all(query), ?)]
  defp insert_all_value(_), do: '?'

  @impl true
  def update(prefix, table, fields, filters, returning) do
    document = Enum.into(fields, %{})
    ["UPDATE ", document, " IN ", quote_table(prefix, table)]
  end

  def update(_repo, _schema_meta, _fields, _filters, _returning, _options) do
    raise "update with multiple filters is not yet implemented"
  end

  @impl true
  def delete(prefix, table, filters, returning) do
    ["REMOVE ", filters, " IN ", quote_table(prefix, table)]
  end

  def delete(_repo, _schema_meta, _filters, _options) do
    raise "delete with multiple filters is not yet implemented"
  end

  #
  # Helpers
  #

  alias Ecto.Query.{BooleanExpr, JoinExpr, QueryExpr}

  defp create_names(%{sources: sources}) do
    create_names(sources, 0, tuple_size(sources)) |> List.to_tuple()
  end

  defp create_names(sources, pos, limit) when pos < limit do
    current =
      case elem(sources, pos) do
        {coll, schema} ->
          name = [String.first(coll) | Integer.to_string(pos)]
          {quote_table(coll), name, schema}

        {:fragment, _, _} ->
          raise "Fragments are not supported."

        %Ecto.SubQuery{} ->
          raise "Subqueries are not supported."
      end

    [current | create_names(sources, pos + 1, limit)]
  end

  defp create_names(_sources, pos, pos) do
    []
  end

  #
  # Query generation
  #

  defp select(%{select: nil}, _sources), do: [" RETURN []"]

  defp select(%{select: %{fields: fields}, distinct: distinct, from: from} = query, sources) do
    [" RETURN ", select_fields(fields, distinct, from, sources, query)]
  end

  defp select_fields(fields, distinct, _from, sources, query) do
    values =
      intersperse_map(fields, ", ", fn
        {_key, value} ->
          [expr(value, sources, query)]

        value ->
          [expr(value, sources, query)]
      end)

    [distinct(distinct, sources, query), "[ ", values | " ]"]
  end

  defp update_fields(%{from: from, updates: updates} = query, sources) do
    {_from, name} = get_source(query, sources, 0, from)

    fields =
      for(
        %{expr: expr} <- updates,
        {op, kw} <- expr,
        {key, value} <- kw,
        do: update_op(op, name, quote_name(key), value, sources, query)
      )

    Enum.intersperse(fields, ", ")
  end

  defp update_op(cmd, name, quoted_key, value, sources, query) do
    value = update_op_value(cmd, name, quoted_key, value, sources, query)
    [quoted_key, ": " | value]
  end

  defp update_op_value(:set, _name, _quoted_key, value, sources, query),
    do: expr(value, sources, query)

  defp update_op_value(:inc, name, quoted_key, value, sources, query),
    do: [name, ?., quoted_key, " + " | expr(value, sources, query)]

  defp update_op_value(:push, name, quoted_key, value, sources, query),
    do: ["PUSH(", name, ?., quoted_key, ", ", expr(value, sources, query), ")"]

  defp update_op_value(:pull, name, quoted_key, value, sources, query),
    do: ["REMOVE_VALUE(", name, ?., quoted_key, ", ", expr(value, sources, query), ", 1)"]

  defp update_op_value(cmd, _name, _quoted_key, _value, _sources, query),
    do: error!(query, "Unknown update operation #{inspect(cmd)} for AQL")

  defp distinct(nil, _sources, _query), do: []
  defp distinct(%QueryExpr{expr: true}, _sources, _query), do: "DISTINCT "
  defp distinct(%QueryExpr{expr: false}, _sources, _query), do: []

  defp distinct(%QueryExpr{expr: exprs}, _sources, query) when is_list(exprs) do
    error!(query, "DISTINCT with multiple fields is not supported by AQL")
  end

  defp from(%{from: from} = query, sources) do
    {coll, name} = get_source(query, sources, 0, from)
    ["FOR ", name, " IN " | coll]
  end

  defp join(%{joins: []}, _sources), do: []

  defp join(%{joins: joins} = query, sources) do
    [
      ?\s
      | intersperse_map(joins, ?\s, fn
          %JoinExpr{on: %QueryExpr{expr: expr}, qual: qual, ix: ix, source: source} ->
            if qual != :inner, do: raise("Only inner joins are supported.")
            {join, name} = get_source(query, sources, ix, source)
            # [join_qual(qual), join, " AS ", name | join_on(qual, expr, sources, query)]
            ["FOR ", name, " IN ", join, " FILTER " | expr(expr, sources, query)]
        end)
    ]
  end

  defp where(%{wheres: wheres} = query, sources) do
    boolean(" FILTER ", wheres, sources, query)
  end

  defp order_by(%{order_bys: []}, _sources), do: []

  defp order_by(%{order_bys: order_bys} = query, sources) do
    [
      " SORT "
      | intersperse_map(order_bys, ", ", fn %QueryExpr{expr: expr} ->
          intersperse_map(expr, ", ", &order_by_expr(&1, sources, query))
        end)
    ]
  end

  defp order_by_expr({dir, expr}, sources, query) do
    str = expr(expr, sources, query)

    case dir do
      :asc -> str
      :desc -> [str | " DESC"]
    end
  end

  defp offset_and_limit(%{offset: nil, limit: nil}, _sources), do: []

  defp offset_and_limit(%{offset: nil, limit: %QueryExpr{expr: expr}} = query, sources) do
    [" LIMIT " | expr(expr, sources, query)]
  end

  defp offset_and_limit(%{offset: %QueryExpr{expr: _}, limit: nil} = query, _) do
    error!(query, "offset can only be used in conjunction with limit")
  end

  defp offset_and_limit(
         %{offset: %QueryExpr{expr: offset_expr}, limit: %QueryExpr{expr: limit_expr}} = query,
         sources
       ) do
    [" LIMIT ", expr(offset_expr, sources, query), ", ", expr(limit_expr, sources, query)]
  end

  defp remove(%{from: from} = query, sources) do
    {coll, name} = get_source(query, sources, 0, from)
    [" REMOVE ", name, " IN " | coll]
  end

  defp update(%{from: from} = query, sources) do
    {coll, name} = get_source(query, sources, 0, from)
    fields = update_fields(query, sources)
    [" UPDATE ", name, " WITH {", fields, "} IN " | coll]
  end

  defp returning(_, %{select: nil}, _sources), do: []

  defp returning(version, query, sources) do
    {source, _, schema} = elem(sources, 0)
    select(query, {{source, version, schema}})
  end

  defp get_source(query, sources, ix, source) do
    {expr, name, _schema} = elem(sources, ix)
    {expr || paren_expr(source, sources, query), name}
  end

  defp boolean(_name, [], _sources, _query), do: []

  defp boolean(name, [%{expr: expr, op: op} | query_exprs], sources, query) do
    [
      name,
      Enum.reduce(query_exprs, {op, paren_expr(expr, sources, query)}, fn
        %BooleanExpr{expr: expr, op: op}, {op, acc} ->
          {op, [acc, operator_to_boolean(op) | paren_expr(expr, sources, query)]}

        %BooleanExpr{expr: expr, op: op}, {_, acc} ->
          {op, [?(, acc, ?), operator_to_boolean(op) | paren_expr(expr, sources, query)]}
      end)
      |> elem(1)
    ]
  end

  defp operator_to_boolean(:and), do: " && "
  defp operator_to_boolean(:or), do: " || "

  defp paren_expr(expr, sources, query) do
    [?(, expr(expr, sources, query), ?)]
  end

  ## Query generation

  binary_ops = [
    ==: " = ",
    !=: " != ",
    <=: " <= ",
    >=: " >= ",
    <: " < ",
    >: " > ",
    +: " + ",
    -: " - ",
    *: " * ",
    /: " / ",
    and: " AND ",
    or: " OR ",
    ilike: " ILIKE ",
    like: " LIKE "
  ]

  @binary_ops Keyword.keys(binary_ops)

  Enum.map(
    binary_ops,
    fn {op, str} ->
      defp handle_call(unquote(op), 2), do: {:binary_op, unquote(str)}
    end
  )

  defp handle_call(fun, _arity), do: {:fun, Atom.to_string(fun)}

  defp expr({:^, [], [idx]}, _sources, _query) do
    [?@ | Integer.to_string(idx + 1)]
  end

  defp expr({{:., _, [{:&, _, [idx]}, field]}, _, []}, sources, _query)
       when is_atom(field) do
    {_, name, _} = elem(sources, idx)
    [name, ?. | quote_name(field)]
  end

  defp expr({:&, _, [idx, fields, _counter]}, sources, query) do
    {source, name, schema} = elem(sources, idx)

    if is_nil(schema) and is_nil(fields) do
      error!(
        query,
        "ArangoDB does not support selecting all fields from #{source} without a schema. " <>
          "Please specify a schema or specify exactly which fields you want to select"
      )
    end

    intersperse_map(fields, ", ", &[name, ?. | quote_name(&1)])
  end

  defp expr({:not, _, [expr]}, sources, query) do
    ["NOT (", expr(expr, sources, query), ?)]
  end

  defp expr({:fragment, _, parts}, sources, query) do
    Enum.map(parts, fn
      {:raw, part} -> part
      {:expr, expr} -> expr(expr, sources, query)
    end)
  end

  defp expr({:is_nil, _, [arg]}, sources, query) do
    [expr(arg, sources, query) | " == NULL"]
  end

  defp expr({:in, _, [_left, []]}, _sources, _query) do
    "FALSE"
  end

  defp expr({:in, _, [left, right]}, sources, query) when is_list(right) do
    args = intersperse_map(right, ?,, &expr(&1, sources, query))
    [expr(left, sources, query), " IN [", args, ?]]
  end

  defp expr({:in, _, [left, {:^, _, [idx, _length]}]}, sources, query) do
    [expr(left, sources, query), " IN @#{idx + 1}"]
  end

  defp expr({:in, _, [left, right]}, sources, query) do
    [expr(left, sources, query), " IN ", expr(right, sources, query)]
  end

  defp expr({fun, _, args}, sources, query) when is_atom(fun) and is_list(args) do
    case handle_call(fun, length(args)) do
      {:binary_op, op} ->
        [left, right] = args
        [op_to_binary(left, sources, query), op | op_to_binary(right, sources, query)]

      {:fun, fun} ->
        [fun, ?(, [], intersperse_map(args, ", ", &expr(&1, sources, query)), ?)]
    end
  end

  defp expr(literal, _sources, _query) when is_binary(literal) do
    [?', escape_string(literal), ?']
  end

  defp expr(list, sources, query) when is_list(list) do
    [?[, intersperse_map(list, ?,, &expr(&1, sources, query)), ?]]
  end

  defp expr(%Decimal{} = decimal, _sources, _query) do
    Decimal.to_string(decimal, :normal)
  end

  defp expr(literal, _sources, _query) when is_integer(literal) do
    Integer.to_string(literal)
  end

  defp expr(literal, _sources, _query) when is_float(literal) do
    Float.to_string(literal)
  end

  defp expr(%Ecto.Query.Tagged{value: value, type: :binary_id}, sources, query) do
    [expr(value, sources, query)]
  end

  defp expr(nil, _sources, _query), do: "NULL"
  defp expr(true, _sources, _query), do: "TRUE"
  defp expr(false, _sources, _query), do: "FALSE"

  # DDL

  alias Ecto.Migration.{Table, Index, Reference, Constraint}

  @creates [:create, :create_if_not_exists]
  @drops [:drop, :drop_if_exists]

  @impl true
  def execute_ddl({command, %Table{name: name, options: options} = table, columns})
      when command in @creates do
    table_name = quote_table(table.prefix, table.name)

    # TODO: use table options
    collection_type =
      case options do
        # document collection
        nil -> 2
        "edge" -> 3
        _ -> raise "Invalid options value `#{options}`."
      end

    Arango.Collection.create(%Arango.Collection{name: name, type: collection_type})
  end

  def execute_ddl({command, %Table{name: name} = table}) when command in @drops do
    Arango.Collection.drop(%Arango.Collection{name: name})
  end

  def execute_ddl({:alter, %Table{} = table, changes}) do
    table_name = quote_table(table.prefix, table.name)
  end

  def execute_ddl({:create, %Index{table: table, columns: columns} = index}) do
    body = make_index(index)
    Arango.Index.create_general(table, Map.put(body, :fields, columns))
  end

  def execute_ddl({:create_if_not_exists, %Index{} = index}) do
    if index.concurrently do
      raise ArgumentError,
            "concurrent index and create_if_not_exists is not supported"
    end
  end

  def execute_ddl({command, %Index{table: table, columns: columns} = index})
      when command in @drops do
    body =
      make_index(index)
      |> Map.put(:fields, columns)
      |> MapSet.new()

    {:ok, %{"error" => false, "indexes" => indexes}} = Arango.Index.indexes(table)

    matching_index = Enum.filter(indexes, &MapSet.subset?(body, MapSet.new(&1)))

    case {command, matching_index} do
      {_, [index]} -> Arango.Index.delete(index["id"])
      {:drop_if_exists, []} -> :ok
      {:drop, []} -> raise "No index found matching #{inspect(index)}"
    end
  end

  defp execute({:alter, _, _}) do
    if options[:log], do: Logger.warn("ALTER command has no effect in ArangoDB.")
    {:ok, nil}
  end

  def execute_ddl({command, _}) do
    raise "#{inspect(__MODULE__)}: unspported DDL operation #{inspect(command)}"
  end

  def execute_ddl(string) when is_binary(string), do: [string]

  def execute_ddl(keyword) when is_list(keyword),
    do: error!(nil, "{inspect __MODULE__} adapter does not support keyword lists in execute")

  defp make_index(%{where: where}) when where != nil,
    do: raise("{inspect __MODULE__} does not support conditional indices.")

  # default to :hash when no index type is specified
  defp make_index(%{using: nil} = index),
    do: make_index(%Ecto.Migration.Index{index | using: :hash}, [])

  defp make_index(%{using: type} = index) when is_atom(type), do: make_index(index, [])

  defp make_index(%{using: {type, opts}} = index) when is_atom(type) and is_list(opts),
    do: make_index(%{index | using: type}, opts)

  defp make_index(%{using: :hash, unique: unique}, options) do
    %{type: "hash", unique: unique, sparse: Keyword.get(options, :sparse, false)}
  end

  defp make_index(%{using: :skip_list, unique: unique}, options) do
    %{type: "skiplist", unique: unique, sparse: Keyword.get(options, :sparse, false)}
  end

  defp make_index(%{using: :fulltext, unique: unique}, options) do
    if unique, do: raise("Fulltext indices cannot be unique.")
    %{type: "fulltext", minLength: Keyword.get(options, :min_length, nil)}
  end

  ## Helpers

  defp op_to_binary({op, _, [_, _]} = expr, sources, query) when op in @binary_ops,
    do: paren_expr(expr, sources, query)

  defp op_to_binary(expr, sources, query), do: expr(expr, sources, query)

  defp quote_name(name) when is_atom(name), do: quote_name(Atom.to_string(name))

  defp quote_name(name) do
    if String.contains?(name, "`"), do: error!(nil, "bad field name #{inspect(name)}")
    [?`, name, ?`]
  end

  defp quote_table(nil, name), do: quote_table(name)
  defp quote_table(prefix, name), do: [quote_table(prefix), ?., quote_table(name)]

  defp quote_table(name) when is_atom(name),
    do: quote_table(Atom.to_string(name))

  defp quote_table(name) do
    if String.contains?(name, "`"), do: error!(nil, "bad table name #{inspect(name)}")

    [?`, name, ?`]
  end

  defp escape_string(value) when is_binary(value) do
    value
    |> :binary.replace("\\", "\\\\", [:global])
    |> :binary.replace("''", "\\'", [:global])
  end

  defp intersperse_map(list, separator, mapper, acc \\ [])
  defp intersperse_map([], _separator, _mapper, acc), do: acc
  defp intersperse_map([elem], _separator, mapper, acc), do: [acc | mapper.(elem)]

  defp intersperse_map([elem | rest], separator, mapper, acc),
    do: intersperse_map(rest, separator, mapper, [acc, mapper.(elem), separator])

  defp ecto_to_db({:array, t}), do: [ecto_to_db(t), ?[, ?]]
  defp ecto_to_db(:id), do: "integer"
  defp ecto_to_db(:serial), do: "serial"
  defp ecto_to_db(:bigserial), do: "bigserial"
  defp ecto_to_db(:binary_id), do: "uuid"
  defp ecto_to_db(:string), do: "varchar"
  defp ecto_to_db(:binary), do: "bytea"
  defp ecto_to_db(:map), do: Application.fetch_env!(:ecto_sql, :postgres_map_type)
  defp ecto_to_db({:map, _}), do: Application.fetch_env!(:ecto_sql, :postgres_map_type)
  defp ecto_to_db(:time_usec), do: "time"
  defp ecto_to_db(:utc_datetime), do: "timestamp"
  defp ecto_to_db(:utc_datetime_usec), do: "timestamp"
  defp ecto_to_db(:naive_datetime), do: "timestamp"
  defp ecto_to_db(:naive_datetime_usec), do: "timestamp"
  defp ecto_to_db(other), do: Atom.to_string(other)

  defp error!(nil, message) do
    raise ArgumentError, message
  end

  defp error!(query, message) do
    raise Ecto.QueryError, query: query, message: message
  end
end
