const { VALID_TIME_UNIT } = require("../lib/globals.js");

function getTotalUsersBy(timeUnit) {
  if (!VALID_TIME_UNIT[timeUnit]) return "Invalid time unit provided";

  return `
    SELECT
      DATE_TRUNC('${VALID_TIME_UNIT[timeUnit]}', CAST(event_created AS TIMESTAMPTZ)) AS ${timeUnit},
      COUNT(DISTINCT user_id) AS calculated_value
    FROM
      events
    WHERE
      (CAST($1 AS TIMESTAMP) IS NULL OR event_created BETWEEN $1 AND SYSDATE)
      AND (CAST($2 AS VARCHAR) IS NULL OR event_name = $2)
      AND (CAST($3 AS VARCHAR) IS NULL OR JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(event_attributes), $3) = $4)
    GROUP BY
      ${timeUnit}
  `;
}

module.exports = {
  getTotalUsersBy,
};
