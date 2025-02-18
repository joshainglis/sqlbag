{
  pkgs,
  lib,
  config,
  ...
}:

{
  packages = [
    pkgs.postgresql_16
    pkgs.mariadb
  ];

  languages.python = {
    enable = true;
    version = "3.12";
    uv = {
      enable = true;
      sync = {
        enable = true;
        allExtras = true;
      };
    };
    venv.enable = true;
  };

  services.postgres.enable = true;
  services.postgres.settings.log_min_messages = "NOTICE";
  services.postgres.settings.client_min_messages = "NOTICE";
  services.postgres.settings.log_min_error_statement = "NOTICE";
  services.mysql.enable = true;
  services.mysql.ensureUsers = [
    {
      name = "root";
      ensurePermissions = {
        "root.*" = "ALL PRIVILEGES";
      };
    }
  ];
  processes.mysql.process-compose.readiness_probe.exec.command = "${lib.getExe' config.services.mysql.package "mariadb-admin"} --protocol socket --socket $MYSQL_UNIX_PORT ping -u root --password=''";

  enterShell = ''
    git --version
  '';

  scripts.reset.exec = ''
    rm -rf $DEVENV_STATE/postgres
    rm -rf $DEVENV_STATE/mysql
  '';
  process.manager.before = ''reset'';

  enterTest =
    let
      mariadb-admin = lib.getExe' config.services.mysql.package "mariadb-admin";
      pg_isready = lib.getExe' config.services.postgres.package "pg_isready";

    in
    ''
      echo "Running tests"
      timeout 30 bash -c "until ${pg_isready} -d template1 -q; do sleep 0.5; done"
      timeout 30 bash -c "until ${mariadb-admin} --protocol socket --socket $MYSQL_UNIX_PORT ping -u root --password=\"\"; do sleep 0.5; done"
      python -m pytest tests
    '';

  git-hooks.hooks = {
    actionlint.enable = true;
    ruff.enable = true;
    ruff-format.enable = true;
    check-toml.enable = true;
    deadnix.enable = true;
    nixfmt-rfc-style.enable = true;
    end-of-file-fixer.enable = true;
    markdownlint.enable = true;
    pyupgrade.enable = true;
    ripsecrets.enable = true;
    trufflehog.enable = true;
  };
}
