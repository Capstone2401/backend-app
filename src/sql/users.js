const { VALID_TIME_UNIT } = require("../lib/globals.js");

function getTotalUsersBy(timeUnit) {
  if (!VALID_TIME_UNIT[timeUnit]) return "Invalid time unit provided";

  return `
    SELECT
      DATE_TRUNC('${VALID_TIME_UNIT[timeUnit]}', CAST(event_created AS TIMESTAMPTZ)) AS ${timeUnit},
      COUNT(DISTINCT events.user_id) AS calculated_value
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

module.exports = {
  getTotalUsersBy,
};
