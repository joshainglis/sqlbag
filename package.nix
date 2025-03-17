{
  lib,
  buildPythonPackage,
  psycopg2,
  pymysql,
  sqlalchemy,
  six,
  flask,
  pendulum,
  packaging,
  hatchling,
  pytestCheckHook,
  pytest-xdist,
  pytest-sugar,
  postgresql,
  postgresqlTestHook,
}:
buildPythonPackage rec {
  pname = "sqlbag";
  version = "0.2.0";
  format = "pyproject";

  doCheck = true;

  src = ./.;

  nativeBuildInputs = [ hatchling ];

  propagatedBuildInputs = [
    sqlalchemy
    six
    packaging
    flask
    pendulum

    psycopg2
    pymysql
  ];

  nativeCheckInputs = [
    pytestCheckHook
    pytest-xdist
    pytest-sugar

    postgresql
    postgresqlTestHook

    flask
    pendulum
  ];

  preCheck = ''
    export postgresqlTestUserOptions="LOGIN SUPERUSER"
  '';

  pytestFlagsArray = [
    "-x"
    "-svv"
    "tests"
  ];

  pythonImportsCheck = [ "sqlbag" ];

  meta = with lib; {
    description = "Handy python code for doing database things";
    homepage = "https://github.com/djrobstep/sqlbag";
    license = with licenses; [ unlicense ];
    maintainers = with maintainers; [ bpeetz ];
  };
}
