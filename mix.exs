defmodule ArangodbEcto.Mixfile do
  use Mix.Project

  @version "0.3.0"
  @url "https://github.com/ArangoDB-Community/arangodb_ecto"

  def project do
    [
      app: :arangodb_ecto,
      name: "ArangoDB.Ecto",
      version: @version,
      elixir: "~> 1.4",
      package: package(),
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      description: description(),
      docs: docs(),
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:ecto_sql, "~> 3.0"},
      {:arango, github: "ijcd/arangoex"},
      {:dialyxir, "~> 0.5", only: :dev, runtime: false},
      {:ex_doc, "~> 0.19", only: :dev, runtime: false}
    ]
  end

  defp description do
    """
    ArangoDB adapter for Ecto
    """
  end

  defp package do
    [
      maintainers: ["Manuel Pöter", "Marcel van Pinxteren"],
      licenses: ["MIT"],
      links: %{"GitHub" => @url},
      files: ["lib", "mix.exs", "README.md"]
    ]
  end

  defp docs do
    [extras: ["README.md"], main: "readme", source_ref: "v#{@version}", source_url: @url]
  end
end
