-- Each user are lambda functions that use thier iam role as users to access redhisft serverless warehouse

GRANT USAGE ON SCHEMA "taxi-model" TO "IAMR:user1";
GRANT SELECT ON ALL TABLES IN SCHEMA "taxi-model" TO "IAMR:user1";

CREATE USER "IAMR:user2" WITH PASSWORD '******';
GRANT USAGE ON SCHEMA "taxi-model" TO "IAMR:user2";
GRANT SELECT ON ALL TABLES IN SCHEMA "taxi-model" TO "IAMR:user2";
GRANT INSERT ON ALL TABLES IN SCHEMA "taxi-model" TO "IAMR:user2";
GRANT TRUNCATE ON ALL TABLES IN SCHEMA "taxi-model" TO "IAMR:user2";

CREATE USER "IAMR:user3" WITH PASSWORD '******';
GRANT USAGE ON SCHEMA "taxi-model" TO "IAMR:user3";
GRANT SELECT ON ALL TABLES IN SCHEMA "taxi-model" TO "IAMR:user3";
GRANT INSERT ON ALL TABLES IN SCHEMA "taxi-model" TO "IAMR:user3";

