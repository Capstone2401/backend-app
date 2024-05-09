import { VALID_TIME_UNIT } from "src/lib/globals";
import { TimeUnit } from "src/types/time";
import { ResponseError } from "src/utils/response-error";

function getAllUserAttributes(): string {
  return `
  SELECT 
    DISTINCT
      JSON_SERIALIZE(user_attributes)
    FROM
      users
  `;
}

function getTotalUsersBy(timeUnit: TimeUnit): string {
  if (!VALID_TIME_UNIT[timeUnit]) {
    throw new ResponseError({
      message: "Invalid time unit provided",
      statusCode: 400,
    });
  }

  return `
    SELECT
      DATE_TRUNC('${timeUnit}', CAST(event_created AS TIMESTAMPTZ)) AS ${timeUnit},
      COUNT(DISTINCT e.user_id) AS calculated_value
    FROM
      events e
    JOIN users u ON e.user_id = u.user_id
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
      AND (CAST($3 AS VARCHAR) IS NULL OR attr_validate($3, JSON_SERIALIZE(e.event_attributes)))
      AND (CAST($4 AS VARCHAR) IS NULL OR attr_validate($4, JSON_SERIALIZE(u.user_attributes)))
    GROUP BY
      ${timeUnit}
  `;
}

export default {
  getTotalUsersBy,
  getAllUserAttributes,
};
