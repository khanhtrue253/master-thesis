--Actual student-tutor-course relations
WITH f AS (
  SELECT DISTINCT student,
    tutor,
    course,
    MIN(begin) AS matching_date,
    1 AS "matched"
  FROM tutoring_booking.appointments
  WHERE status = 'accepted'
    AND type = 'lesson'
    AND course < 100
    AND course > 0
    AND course IS NOT NULL
    AND hidden_student = 0
    AND hidden_tutor = 0
    AND canceled_at IS NULL
  GROUP BY student,
    tutor,
    course,
    1
),
--Hypothetical student-tutor-course relation, if they had intro but never had lesson
u as (
  With a AS (
    SELECT DISTINCT student,
      tutor,
      course,
      "end",
      0 as "matched"
    FROM tutoring_booking.appointments
    WHERE type = 'intro'
      AND course < 100
      AND course > 0
      AND course IS NOT NULL
      AND hidden_student = 0
      AND hidden_tutor = 0
  )
  SELECT *
  FROM a
  WHERE (a.student, a.tutor, a.course) NOT IN (
      SELECT f.student,
        f.tutor,
        f.course
      FROM f
    )
),
--Hypothetical student-tutor-course relation, if they chatted with each other but never had lesson (only if we are sure of the course they were intending to do together)
c as (
  WITH snapshot AS (
    select MAX(snapshot_id) as id
    from "tutoring_message".messages
  ),
  e as (
    SELECT a.room_id AS room_id,
      a.country,
      first_student_timestamp,
      first_tutor_timestamp,
      date_diff(
        'minute',
        first_student_timestamp,
        first_tutor_timestamp
      ) as response_time_in_minutes,
      first_tutor_timestamp IS NOT NULL
      OR isFirstAttemptAccepted as responded,
      message_as_first_contact,
      student_created_at,
      date_diff(
        'minute',
        student_created_at,
        first_student_timestamp
      ) as student_time_to_outreach,
      tutor_created_at,
      first_message_created_at
    FROM (
        SELECT room_id,
          role,
          country,
          first_message_created_at,
          IF(
            timestamp IS NULL
            OR first_message_created_at < timestamp,
            first_message_created_at,
            timestamp
          ) as first_student_timestamp,
          timestamp IS NOT NULL
          AND first_message_created_at > timestamp as message_as_first_contact,
          isFirstAttemptAccepted,
          date_parse(
            SUBSTRING(student_created_at, 1, 19),
            '%Y-%m-%dT%k:%i:%s'
          ) as student_created_at
        FROM (
            SELECT room_id,
              u.role,
              u.country,
              MIN(m.created_at) as first_message_created_at,
              MIN(u.created_at) as student_created_at
            FROM AwsDataCatalog.tutoring_message.messages as m
              LEFT JOIN snapshot s ON true
              LEFT JOIN AwsDataCatalog.tutoring_user.users as u ON m.user_id = u._id
            WHERE u.role = 'Student'
              AND m.snapshot_id = s.id
            GROUP BY room_id,
              u.role,
              u.country
          ) as a
          JOIN (
            SELECT ta.student,
              ta.tutor,
              booked_by_table.booked_by AS booked_by,
              SUM(CAST(status = 'accepted' AS INTEGER)) > 1 as isFirstAttemptAccepted,
              MIN(created_at) as timestamp,
              CONCAT(
                CAST(ta.student AS VARCHAR),
                ':',
                CAST(ta.tutor AS VARCHAR)
              ) as room_id_1,
              CONCAT(
                CAST(ta.tutor AS VARCHAR),
                ':',
                CAST(ta.student AS VARCHAR)
              ) as room_id_2
            FROM AwsDataCatalog.tutoring_booking.appointments ta
              LEFT JOIN (
                SELECT u._id,
                  u.role as booked_by
                FROM AwsDataCatalog.tutoring_user.users as u
              ) as booked_by_table ON booked_by_table._id = ta.booked_by
              LEFT JOIN snapshot s ON true
            WHERE booked_by_table.booked_by = 'Student'
              AND ta.snapshot_id = s.id
            GROUP BY student,
              tutor,
              booked_by_table.booked_by
          ) as b ON a.room_id = b.room_id_1
          OR a.room_id = b.room_id_2
      ) as a
      FULL JOIN (
        SELECT room_id,
          role,
          country,
          IF(
            timestamp IS NULL
            OR first_message_created_at < timestamp,
            first_message_created_at,
            timestamp
          ) as first_tutor_timestamp,
          date_parse(
            SUBSTRING(tutor_created_at, 1, 19),
            '%Y-%m-%dT%k:%i:%s'
          ) as tutor_created_at
        FROM (
            SELECT room_id,
              u.role,
              u.country,
              MIN(m.created_at) as first_message_created_at,
              MIN(u.created_at) as tutor_created_at
            FROM AwsDataCatalog.tutoring_message.messages as m
              LEFT JOIN snapshot s ON true
              LEFT JOIN AwsDataCatalog.tutoring_user.users as u ON m.user_id = u._id
            WHERE u.role = 'Tutor'
              AND m.snapshot_id = s.id
            GROUP BY room_id,
              u.role,
              u.country
          ) as a
          JOIN (
            SELECT student,
              tutor,
              booked_by_table.booked_by AS booked_by,
              CAST(
                SUM(CAST(status = 'accepted' AS INTEGER)) > 1 AS INTEGER
              ) as isFirstAttemptAccepted,
              MIN(ta.created_at) as timestamp,
              CONCAT(
                CAST(ta.student AS VARCHAR),
                ':',
                CAST(ta.tutor AS VARCHAR)
              ) as room_id_1,
              CONCAT(
                CAST(ta.tutor AS VARCHAR),
                ':',
                CAST(ta.student AS VARCHAR)
              ) as room_id_2
            FROM AwsDataCatalog.tutoring_booking.appointments ta
              LEFT JOIN snapshot s ON true
              LEFT JOIN (
                SELECT u._id,
                  u.role as booked_by
                FROM AwsDataCatalog.tutoring_user.users as u
              ) as booked_by_table ON booked_by_table._id = ta.booked_by
            WHERE booked_by_table.booked_by = 'Tutor'
              AND ta.snapshot_id = s.id
            GROUP BY student,
              tutor,
              booked_by_table.booked_by
          ) as b ON a.room_id = b.room_id_1
          OR a.room_id = b.room_id_2
      ) as b ON a.room_id = b.room_id
  ),
  b as (
    select CAST(split(room_id, ':') [ 1 ] AS decimal) AS tutor,
      CAST(split(room_id, ':') [ 2 ] AS decimal) AS student,
      first_message_created_at AS matching_date
    from e
    where (
        CAST(split(room_id, ':') [ 1 ] AS decimal),
        CAST(split(room_id, ':') [ 2 ] AS decimal)
      ) NOT IN (
        SELECT tutor,
          student
        FROM f
      )
      AND CAST(split(room_id, ':') [ 1 ] AS decimal) IS NOT NULL
  ),
  d AS (
    SELECT DISTINCT b.tutor AS tutor,
      b.student AS student,
      b.matching_date AS matching_date,
      ta.course AS course
    FROM b
      LEFT JOIN algorithm_search.tutor_course_offering tc ON b.tutor = tc.tutor
      LEFT JOIN (
        SELECT DISTINCT student,
          course
        FROM tutoring_booking.appointments
        WHERE hidden_student = 0
      ) AS ta ON b.student = ta.student
    WHERE tc.course_code = ta.course
  )
  SELECT tutor,
    student,
    course,
    matching_date,
    0 as "matched"
  FROM (
      SELECT tutor,
        student,
        course,
        matching_date,
        COUNT(*) OVER (PARTITION BY tutor, student) AS pair_count
      FROM d
    ) t
  WHERE pair_count = 1
),
--Combined student-tutor-course relations
k as (
  select *
  from f
  UNION
  select *
  from u
  UNION
  select *
  from c
),
--Actual Student-tutor-course relations, with the connection stats (number of students/tutors before matching)
r as (
  SELECT k.student AS student,
    k.tutor AS tutor,
    k.course AS course,
    k.matching_date AS matching_date,
    k.matched AS "matched",
    COUNT(
      DISTINCT CASE
        WHEN k4."matched" = 1 THEN k4.student
      END
    ) AS tutor_previous_number_of_students,
    COUNT(
      DISTINCT CASE
        WHEN k5."matched" = 1 THEN k5.student
      END
    ) AS tutor_course_previous_number_of_students
  FROM k
    LEFT JOIN k AS k4 ON k.student != k4.student
    AND k.tutor = k4.tutor
    AND k.matching_date > k4.matching_date
    LEFT JOIN k AS k5 ON k.student != k5.student
    AND k.tutor = k5.tutor
    AND k.matching_date > k5.matching_date
    AND k.course = k5.course
  GROUP BY k.student,
    k.tutor,
    k.course,
    k.matching_date,
    k.matched
),
--Most recent snapshot *prior to the student-course-relation
m AS (
  SELECT id,
    MAX(snapshot_id) AS snapshot_id
  FROM tutoring_booking.appointments
  GROUP BY id
),
--All lessons from the most recent server snapshot
lesson AS (
  SELECT m.id,
    "type",
    begin,
    "end",
    DATE_DIFF('minute', begin, "end") AS duration,
    series,
    ta.student AS student,
    ta.tutor AS tutor,
    booked_by,
    status,
    online,
    course,
    ta.created_at,
    accepted_at,
    canceled_at,
    first_booking,
    country,
    ta.snapshot_id
  FROM m
    LEFT JOIN tutoring_booking.appointments ta ON m.id = ta.id
    AND m.snapshot_id = ta.snapshot_id
  WHERE course IS NOT NULL
    AND course < 1000
    AND course > 0
    AND ta.type = 'lesson'
),
--All intros from most recent server snapshot
intro AS (
  SELECT m.id,
    "type",
    begin,
    ta.student AS student,
    ta.tutor AS tutor,
    booked_by,
    status,
    course,
    ta.created_at,
    ta.snapshot_id
  FROM m
    LEFT JOIN tutoring_booking.appointments ta ON m.id = ta.id
    AND m.snapshot_id = ta.snapshot_id
  WHERE course IS NOT NULL
    AND course < 1000
    AND course > 0
    AND ta."type" = 'intro'
),
--*Aggregated lesson stats from student-tutor-course relation
lesson_stat AS (
  SELECT r.student AS student,
    r.tutor AS tutor,
    r.course AS course,
    r.matching_date AS matching_date,
    r.matched as matched,
    -- Count of tutoring lesson requests
    SUM(
      CASE
        WHEN lesson.type = 'lesson'
        AND lesson.tutor = r.tutor
        AND lesson.begin < r.matching_date THEN 1 ELSE 0
      END
    ) AS tutor_total_appointment_requests,
    SUM(
      CASE
        WHEN lesson.type = 'lesson'
        AND lesson.tutor = r.tutor
        AND lesson.begin < r.matching_date
        AND lesson.course = r.course THEN 1 ELSE 0
      END
    ) AS tutor_course_appointment_requests,
    SUM(
      CASE
        WHEN lesson.type = 'lesson'
        AND lesson.tutor = r.tutor
        AND lesson.status = 'accepted'
        AND lesson.begin < r.matching_date THEN 1 ELSE 0
      END
    ) AS tutor_total_accepted_lessons,
    SUM(
      CASE
        WHEN lesson.type = 'lesson'
        AND lesson.tutor = r.tutor
        AND lesson.status = 'accepted'
        AND lesson.begin < r.matching_date
        AND lesson.course = r.course THEN 1 ELSE 0
      END
    ) AS tutor_course_accepted_lessons,
    coalesce(
      (
        SELECT ROUND(AVG(lesson.duration), 2)
        FROM lesson
        WHERE lesson.type = 'lesson'
          AND lesson.status = 'accepted'
          AND lesson.tutor = r.tutor
          AND lesson.begin < r.matching_date
      ),
      CASE
        When r.course = 61 THEN 45
        When r.course = 36 THEN 75
        WHEN r.course = 24 THEN 90 ELSE 60
      END
    ) AS tutor_average_duration,
    coalesce(
      (
        SELECT ROUND(AVG(lesson.duration), 2)
        FROM lesson
        WHERE lesson.type = 'lesson'
          AND lesson.status = 'accepted'
          AND lesson.student = r.student
          AND lesson.begin < r.matching_date
      ),
      CASE
        When r.course = 61 THEN 45
        When r.course = 36 THEN 75
        WHEN r.course = 24 THEN 90 ELSE 60
      END
    ) AS student_average_duration,
    -- Leave matching date for # of days from tutor creation
    min(
      IF(
        lesson.status = 'accepted'
        AND lesson.tutor = r.tutor
        AND lesson.type = 'lesson',
        lesson.BEGIN,
        NULL
      )
    ) AS tutor_first_lesson_date,
    -- Count of student's lesson requests
    SUM(
      CASE
        WHEN lesson.type = 'lesson'
        AND lesson.student = r.student
        AND lesson.begin < r.matching_date THEN 1 ELSE 0
      END
    ) AS student_previous_requested_lessons,
    SUM(
      CASE
        WHEN lesson.type = 'lesson'
        AND lesson.student = r.student
        AND lesson.status = 'accepted'
        AND lesson.begin < r.matching_date THEN 1 ELSE 0
      END
    ) AS student_previous_lessons,
    -- Number of students the tutor has
    r.tutor_previous_number_of_students AS tutor_previous_number_of_students,
    r.tutor_course_previous_number_of_students AS tutor_course_previous_number_of_students,
    SUM(
      CASE
        WHEN r.matched = 1 THEN CASE
          WHEN lesson.type = 'lesson'
          AND lesson.status = 'accepted'
          AND lesson.course = r.course
          AND lesson.tutor = r.tutor
          AND lesson.student = r.student THEN 1 ELSE 0
        END ELSE 0
      END
    ) AS tutor_student_number_of_lessons
  FROM r,
    lesson
  GROUP BY r.student,
    r.tutor,
    r.course,
    r.matching_date,
    r.tutor_previous_number_of_students,
    r.tutor_course_previous_number_of_students,
    r.matched,
    lesson.country
),
--*Aggregated intro stats from student-tutor-course relation
intro_stat AS (
  SELECT r.student AS student,
    r.tutor AS tutor,
    r.course AS course,
    r.matching_date AS matching_date,
    -- Intro lesson requests
    COUNT(
      DISTINCT CASE
        WHEN intro."type" = 'intro'
        AND intro.tutor = r.tutor
        AND intro.student != r.student
        AND intro.begin < r.matching_date THEN intro.student
      END
    ) AS tutor_total_intro_requests,
    SUM(
      CASE
        WHEN intro."type" = 'intro'
        AND intro.tutor = r.tutor
        AND intro.student != r.student
        AND intro.status = 'accepted'
        AND intro.begin < r.matching_date THEN 1 ELSE 0
      END
    ) AS tutor_total_accepted_intro_lessons,
    -- Student intro lesson requests
    SUM(
      CASE
        WHEN intro.type = 'intro'
        AND intro.student = r.student
        AND intro.tutor != r.tutor
        AND intro.begin < r.matching_date THEN 1 ELSE 0
      END
    ) AS student_total_intro_requests,
    SUM(
      CASE
        WHEN intro.type = 'intro'
        AND intro.student = r.student
        AND intro.tutor != r.tutor
        AND intro.status = 'accepted'
        AND intro.begin < r.matching_date THEN 1 ELSE 0
      END
    ) AS student_total_accepted_intro_lessons
  FROM r,
    intro
  GROUP BY r.student,
    r.tutor,
    r.course,
    r.tutor_previous_number_of_students,
    r.matching_date,
    r.tutor_course_previous_number_of_students
),
--*Check whether the tutor and the student had an intro before matching
intro_lesson_check AS (
  SELECT r.student,
    r.tutor,
    r.course,
    MAX(
      CASE
        WHEN intro.status = 'accepted' THEN 1 ELSE 0
      END
    ) AS student_tutor_has_intro
  FROM r
    LEFT JOIN intro ON r.student = intro.student
    AND r.tutor = intro.tutor
    AND r.course = intro.course
  GROUP BY r.student,
    r.tutor,
    r.course
  ORDER by student DESC
),
--All snapshots of tutor availability
"time" as (
  With a as (
    SELECT user,
      from_unixtime(CAST(begin AS BIGINT) / 1000000000) AS begin_time,
      from_unixtime(CAST("end" AS BIGINT) / 1000000000) AS end_time,
      day_of_week(
        from_unixtime(CAST(begin AS BIGINT) / 1000000000)
      ) AS day_of_week_begin,
      hour(
        from_unixtime(CAST(begin AS BIGINT) / 1000000000)
      ) AS hour_begin,
      day_of_week(
        from_unixtime(CAST("end" AS BIGINT) / 1000000000)
      ) AS day_of_week_end,
      hour(
        from_unixtime(CAST("end" AS BIGINT) / 1000000000)
      ) AS hour_end,
      snapshot_id
    FROM tutoring_booking.availabilities
  ),
  b as (
    Select user,
      CASE
        When day_of_week_begin = day_of_week_end THEN hour_end - hour_begin
        WHEN day_of_week_begin > day_of_week_end
        AND day_of_week_begin = 7
        AND hour_begin = hour_end THEN (hour_end + 24 - hour_begin) * abs(day_of_week_end - day_of_week_begin + 7)
        WHEN day_of_week_begin > day_of_week_end
        AND day_of_week_begin = 7
        AND hour_begin != hour_end THEN (hour_end + 24 - hour_begin) + 24 * abs(day_of_week_end - day_of_week_begin + 6)
        WHEN day_of_week_begin > day_of_week_end
        AND hour_begin = hour_end THEN (hour_end + 24 - hour_begin) * abs(day_of_week_begin - day_of_week_end)
        WHEN day_of_week_begin > day_of_week_end
        AND hour_begin != hour_end THEN (hour_end + 24 - hour_begin) + 24 * abs(day_of_week_end - day_of_week_begin)
        WHEN hour_begin = hour_end
        AND day_of_week_begin < day_of_week_end THEN (hour_end + 24 - hour_begin)
      End as duration,
      begin_time,
      end_time,
      day_of_week_begin,
      day_of_week_end,
      snapshot_id
    from a
  ),
  c AS (
    SELECT user,
      day_of_week_begin,
      day_of_week_end,
      snapshot_id,
      MAX(duration) AS duration
    FROM b
    GROUP BY "user",
      day_of_week_begin,
      day_of_week_end,
      snapshot_id
  )
  Select user AS tutor,
    sum(duration) AS total_availability_in_hours,
    CAST(
      SUBSTR(snapshot_id, 16, 4) || '-' || -- Extract year
      SUBSTR(snapshot_id, 21, 2) || '-' || -- Extract month
      SUBSTR(snapshot_id, 24, 2) || ' ' || -- Extract day
      SUBSTR(snapshot_id, 27, 2) || ':' || -- Extract hour
      SUBSTR(snapshot_id, 30, 2) || ':' || -- Extract minute
      '00' AS TIMESTAMP
    ) AS snapshot_id
  from c
  group by user,
    snapshot_id
),
--Picking most recent snapshot before student-tutor-course match    
timeshot AS (
  SELECT r.student AS student,
    r.tutor AS tutor,
    r.course AS course,
    MAX(snapshot_id) AS date_of_record
  FROM r
    LEFT JOIN "time" ON r.tutor = "time".tutor
    AND r.matching_date > "time".snapshot_id
  GROUP BY r.student,
    r.tutor,
    r.course
),
--*Final tutor availability in hours for student-tutor-course relation
availability as (
  Select timeshot.student AS student,
    timeshot.tutor AS tutor,
    timeshot.course AS course,
    coalesce("time".total_availability_in_hours, 0) AS availabile_hours,
    timeshot.date_of_record
  From timeshot
    LEFT JOIN "time" on timeshot.tutor = "time".tutor
    AND timeshot.date_of_record = "time".snapshot_id
),
--All rooms for students-tutor to contact
tutor_response AS (
  -----Most recent snapshot 
  WITH snapshot AS (
    select MAX(snapshot_id) as id
    from "tutoring_message".messages
  ),
  -----All chatrooms
  room_overview AS (
    SELECT a.room_id AS room_id,
      first_student_timestamp,
      first_tutor_timestamp,
      date_diff(
        'minute',
        first_student_timestamp,
        first_tutor_timestamp
      ) as response_time_in_minutes,
      first_tutor_timestamp IS NOT NULL
      OR isFirstAttemptAccepted as responded,
      message_as_first_contact,
      student_created_at,
      date_diff(
        'minute',
        student_created_at,
        first_student_timestamp
      ) as student_time_to_outreach,
      tutor_created_at,
      first_message_created_at
    FROM (
        SELECT room_id,
          role,
          country,
          first_message_created_at,
          IF(
            timestamp IS NULL
            OR first_message_created_at < timestamp,
            first_message_created_at,
            timestamp
          ) as first_student_timestamp,
          timestamp IS NOT NULL
          AND first_message_created_at > timestamp as message_as_first_contact,
          isFirstAttemptAccepted,
          date_parse(
            SUBSTRING(student_created_at, 1, 19),
            '%Y-%m-%dT%k:%i:%s'
          ) as student_created_at
        FROM (
            SELECT room_id,
              u.role,
              u.country,
              MIN(m.created_at) as first_message_created_at,
              MIN(u.created_at) as student_created_at
            FROM AwsDataCatalog.tutoring_message.messages as m
              LEFT JOIN snapshot s ON true
              LEFT JOIN AwsDataCatalog.tutoring_user.users as u ON m.user_id = u._id
            WHERE u.role = 'Student'
              AND m.snapshot_id = s.id
            GROUP BY room_id,
              u.role,
              u.country
          ) as a
          JOIN (
            SELECT ta.student,
              ta.tutor,
              booked_by_table.booked_by AS booked_by,
              SUM(CAST(status = 'accepted' AS INTEGER)) > 1 as isFirstAttemptAccepted,
              MIN(created_at) as timestamp,
              CONCAT(
                CAST(ta.student AS VARCHAR),
                ':',
                CAST(ta.tutor AS VARCHAR)
              ) as room_id_1,
              CONCAT(
                CAST(ta.tutor AS VARCHAR),
                ':',
                CAST(ta.student AS VARCHAR)
              ) as room_id_2
            FROM AwsDataCatalog.tutoring_booking.appointments ta
              LEFT JOIN (
                SELECT u._id,
                  u.role as booked_by
                FROM AwsDataCatalog.tutoring_user.users as u
              ) as booked_by_table ON booked_by_table._id = ta.booked_by
              LEFT JOIN snapshot s ON true
            WHERE booked_by_table.booked_by = 'Student'
              AND ta.snapshot_id = s.id
            GROUP BY student,
              tutor,
              booked_by_table.booked_by
          ) as b ON a.room_id = b.room_id_1
          OR a.room_id = b.room_id_2
      ) as a
      FULL JOIN (
        SELECT room_id,
          role,
          country,
          IF(
            timestamp IS NULL
            OR first_message_created_at < timestamp,
            first_message_created_at,
            timestamp
          ) as first_tutor_timestamp,
          date_parse(
            SUBSTRING(tutor_created_at, 1, 19),
            '%Y-%m-%dT%k:%i:%s'
          ) as tutor_created_at
        FROM (
            SELECT room_id,
              u.role,
              u.country,
              MIN(m.created_at) as first_message_created_at,
              MIN(u.created_at) as tutor_created_at
            FROM AwsDataCatalog.tutoring_message.messages as m
              LEFT JOIN snapshot s ON true
              LEFT JOIN AwsDataCatalog.tutoring_user.users as u ON m.user_id = u._id
            WHERE u.role = 'Tutor'
              AND m.snapshot_id = s.id
            GROUP BY room_id,
              u.role,
              u.country
          ) as a
          JOIN (
            SELECT student,
              tutor,
              booked_by_table.booked_by AS booked_by,
              CAST(
                SUM(CAST(status = 'accepted' AS INTEGER)) > 1 AS INTEGER
              ) as isFirstAttemptAccepted,
              MIN(ta.created_at) as timestamp,
              CONCAT(
                CAST(ta.student AS VARCHAR),
                ':',
                CAST(ta.tutor AS VARCHAR)
              ) as room_id_1,
              CONCAT(
                CAST(ta.tutor AS VARCHAR),
                ':',
                CAST(ta.student AS VARCHAR)
              ) as room_id_2
            FROM AwsDataCatalog.tutoring_booking.appointments ta
              LEFT JOIN snapshot s ON true
              LEFT JOIN (
                SELECT u._id,
                  u.role as booked_by
                FROM AwsDataCatalog.tutoring_user.users as u
              ) as booked_by_table ON booked_by_table._id = ta.booked_by
            WHERE booked_by_table.booked_by = 'Tutor'
              AND ta.snapshot_id = s.id
            GROUP BY student,
              tutor,
              booked_by_table.booked_by
          ) as b ON a.room_id = b.room_id_1
          OR a.room_id = b.room_id_2
      ) as b ON a.room_id = b.room_id
  ) -----Simplified tutor response table (I won't go through Eike's code LMAO)
  Select CAST(
      split(room_overview.room_id, ':') [ 1 ] AS decimal(20, 0)
    ) AS tutor,
    CAST(
      split(room_overview.room_id, ':') [ 2 ] AS decimal(20, 0)
    ) AS student,
    room_overview.response_time_in_minutes AS response_time_in_minutes,
    room_overview.responded AS responded,
    room_overview.message_as_first_contact AS message_as_first_contact,
    first_student_timestamp
  From room_overview
),
--*General room response behavior of tutors
response_stat as (
  SELECT rs.student,
    rs.tutor,
    rs.course,
    rs.matching_date,
    CASE
      WHEN rs.count_responses = 0
      AND total_responses <= 3 THEN 156
      WHEN rs.count_responses = 0
      AND total_responses > 3 THEN 4320
      WHEN rs.count_responses != 0
      AND median_response_time IS NOT NULL THEN median_response_time
      WHEN rs.count_responses != 0
      AND median_response_time IS NULL THEN 156
    END AS median_response_time
  FROM (
      SELECT r.student AS student,
        r.tutor AS tutor,
        r.course AS course,
        r.tutor_previous_number_of_students AS tutor_previous_number_of_students,
        r.matching_date AS matching_date,
        COUNT(
          CASE
            WHEN tutor_response.responded = true THEN 1
          END
        ) AS count_responses,
        COUNT(tutor_response.responded) AS total_responses,
        approx_percentile(
          CASE
            WHEN tutor_response.response_time_in_minutes IS NOT NULL THEN tutor_response.response_time_in_minutes
          END,
          0.5
        ) AS median_response_time
      FROM r
        LEFT JOIN tutor_response ON tutor_response.tutor = r.tutor
        AND r.matching_date > tutor_response.first_student_timestamp
      GROUP BY r.student,
        r.tutor,
        r.course,
        r.tutor_previous_number_of_students,
        r.matching_date
    ) AS rs
),
--*Count the number of relations tutor has before the match
number_of_relations AS (
with rooms AS (select 
    CAST(SPLIT(room_id, ':')[1] as decimal) AS first_id,
    CAST(SPLIT(room_id, ':')[2] as decimal) AS second_id,
    created_at 
from tutoring_message.messages 
where snapshot_id = (
    select MAX(snapshot_id) as id
    from "tutoring_message".messages
  )),
  
timestamp AS (
select first_id, second_id, min(created_at) AS created_at
from rooms
group by first_id, second_id),

identity as (select _id, role from tutoring_user.users),

get_roles as (select first_id, identity.role AS first_role, second_id, i.role AS second_role, timestamp.created_at AS created_at
from timestamp LEFT JOIN identity on timestamp.first_id = identity._id LEFT JOIN identity AS i On timestamp.second_id = i._id),

final AS (select 
CASE when first_role = 'Tutor' THEN first_id ELSE second_id END AS tutor,
CASE when first_role = 'Student' THEN first_id ELSE second_id END AS student,
created_at
from get_roles)

select r.student, r.tutor, r.course, r.matching_date, count(DISTINCT final.student) AS number_of_relations
from r LEFT JOIN final on r.tutor = final.tutor AND r.matching_date > final.created_at AND r.student != final.student
group by r.student, r.tutor, r.course, r.matching_date),
--Get all lessons to create match of intros to student
all_lesson as (
  SELECT ta."type" AS "type",
    ta."begin" AS "begin",
    ta.student AS student,
    ta.tutor AS tutor,
    ta.status AS status,
    ta.course AS course
  FROM m
    LEFT JOIN tutoring_booking.appointments ta ON m.id = ta.id
  WHERE ta.course IS NOT NULL
    AND ta.course < 1000
    AND ta.course > 0
),
--*Count the number of [intro to students] the tutor has
successful_intros AS (
  with a as(
    select distinct c.student As student,
      c.tutor as tutor,
      c."begin" AS "begin",
      c.course AS course
    from (
        select type AS type,
          min(begin) as begin,
          student AS student,
          tutor,
          status,
          course
        from all_lesson
        group by type,
          student,
          tutor,
          status,
          course
      ) AS c
      join all_lesson AS b on c.tutor = b.tutor
      AND c.begin > b.begin
      AND c.type != b.type
      AND c.course = b.course
    where c.type = 'lesson'
      AND c.status = 'accepted'
      AND b.status = 'accepted'
      And c.student = b.student
  )
  select r.student,
    r.tutor,
    r.course,
    r.matching_date,
    count(distinct all_lesson.student) AS intros_leading_to_lesson
  from r
    LEFT JOIN all_lesson on r.tutor = all_lesson.tutor
    AND all_lesson.begin < r.matching_date
  group by r.student,
    r.tutor,
    r.course,
    r.matching_date
),
--*Count the number of administrative cancellations the tutor had (a new appointment was rebooked within 7 days & accepted from the previously cancelled one)
cancellations AS (
  with q as (
    select DISTINCT all_lesson."begin" AS "begin",
      all_lesson.student AS student,
      all_lesson.tutor AS tutor,
      all_lesson.course AS course
    from all_lesson
      JOIN all_lesson AS b on all_lesson.type = b.type
      AND all_lesson.type = 'lesson'
      AND all_lesson.status = 'accepted'
      AND b.status = 'canceled'
      AND all_lesson.tutor = b.tutor
      AND all_lesson.student = b.student
      AND all_lesson.course = b.course
      AND all_lesson.begin <= b.begin + INTERVAL '7' DAY
      AND all_lesson.begin > b.begin
  )
  select r.student AS student,
    r.tutor AS tutor,
    r.course AS course,
    r.matching_date AS matching_date,
    count(q.student) AS tutor_cancellations
  from r
    LEFT JOIN q ON q.begin < r.matching_date
    AND q.tutor = r.tutor
  GROUP BY r.student,
    r.tutor,
    r.course,
    r.matching_date
),
--*Check online lesson rate of student at time of match
online_rate AS (
  With n AS (
    SELECT DISTINCT k.student AS student_a,
      k.tutor AS tutor_a,
      k.begin AS begin_a,
      t.student AS student_b,
      t.tutor AS tutor_b,
      t.online AS online_b,
      t.begin AS begin_b
    FROM (
        select student,
          tutor,
          course,
          min(begin) AS "begin"
        from lesson
        group by student,
          tutor,
          course
      ) AS k
      LEFT JOIN lesson AS t ON k.student = t.student
      AND t.type = 'lesson'
      AND k.begin > t.begin
    Where t.type = 'lesson'
      AND t.begin IS NOT NULL
  )
  Select r.student AS student,
    r.tutor AS tutor,
    r.course AS course,
    r.matching_date AS matching_date,
    coalesce(count(n.online_b), 0) AS total_lessons,
    coalesce(sum(n.online_b), 0) AS total_online,
    CASE
      when coalesce(count(n.online_b), 0) = 0 THEN 0.0 ELSE CAST(coalesce(sum(n.online_b), 0) AS decimal(20, 2)) / CAST(coalesce(count(n.online_b), 0) AS decimal(20, 2))
    END AS online_lesson_rate
  from r
    LEFT JOIN n on r.student = n.student_a
    AND r.tutor = n.tutor_a
    AND r.matching_date = n.begin_a
  group by r.student,
    r.tutor,
    r.course,
    r.matching_date
)
Select DISTINCT lesson_stat.*,
  intro_stat.tutor_total_intro_requests,
  intro_stat.tutor_total_accepted_intro_lessons,
  intro_stat.student_total_intro_requests,
  intro_stat.student_total_accepted_intro_lessons,
  intro_lesson_check.student_tutor_has_intro,
  availability.availabile_hours,
  response_stat.median_response_time,
  successful_intros.intros_leading_to_lesson,
  cancellations.tutor_cancellations,
  online_rate.online_lesson_rate,
  coalesce(number_of_relations.number_of_relations, 0) AS number_of_relations
from lesson_stat
  LEFT JOIN intro_stat ON intro_stat.tutor = lesson_stat.tutor
  AND intro_stat.student = lesson_stat.student
  AND intro_stat.course = lesson_stat.course
  AND intro_stat.matching_date = lesson_stat.matching_date
  LEFT JOIN intro_lesson_check ON intro_lesson_check.tutor = lesson_stat.tutor
  AND intro_lesson_check.student = lesson_stat.student
  AND intro_lesson_check.course = lesson_stat.course
  LEFT JOIN availability ON availability.student = lesson_stat.student
  AND availability.tutor = lesson_stat.tutor
  AND availability.course = lesson_stat.course
  LEFT JOIN response_stat ON response_stat.student = lesson_stat.student
  AND response_stat.tutor = lesson_stat.tutor
  AND response_stat.course = lesson_stat.course
  LEFT JOIN number_of_relations ON number_of_relations.student = lesson_stat.student
  AND number_of_relations.tutor = lesson_stat.tutor
  AND number_of_relations.course = lesson_stat.course
  LEFT JOIN successful_intros ON successful_intros.student = lesson_stat.student
  AND successful_intros.tutor = lesson_stat.tutor
  AND successful_intros.course = lesson_stat.course
  AND successful_intros.matching_date = lesson_stat.matching_date
  LEFT JOIN cancellations ON cancellations.student = lesson_stat.student
  AND cancellations.tutor = lesson_stat.tutor
  AND cancellations.course = lesson_stat.course
  AND cancellations.matching_date = lesson_stat.matching_date
  LEFT JOIN online_rate ON online_rate.student = lesson_stat.student
  AND online_rate.tutor = lesson_stat.tutor
  AND online_rate.course = lesson_stat.course
  AND online_rate.matching_date = lesson_stat.matching_date
where (
    lesson_stat.matched = 1
    AND lesson_stat.tutor_student_number_of_lessons != 0
  )
  OR (
    lesson_stat.matched = 0
    AND lesson_stat.tutor_student_number_of_lessons = 0
  )
  AND (
    lesson_stat.tutor_total_accepted_lessons = 0
    OR lesson_stat.tutor_first_lesson_date IS NOT NULL
  )
