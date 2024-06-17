CREATE TABLE "algorithm_search"."nl_student" WITH (format = 'parquet') AS With user_table as(
  WITH snapshot AS (
    SELECT Max(snapshot_id) AS id
    FROM "tutoring_message".messages
  )
  SELECT u._id,
    u.role,
    Date_parse(
      Substring(u.created_at, 1, 19),
      '%Y-%m-%dT%k:%i:%s'
    ) AS created_at,
    Date_parse(
      Substring(u.last_active_at, 1, 19),
      '%Y-%m-%dT%k:%i:%s'
    ) AS last_active_at,
    u.country,
    u.state,
    heard_about_text AS heard_about,
    heard_about_other,
    admin_label.date AS admin_label_date,
    admin_label.label_id AS admin_label_id,
    Date_parse(
      Substring(u.created_at, 1, 19),
      '%Y-%m-%dT%k:%i:%s'
    ) AS registration_date,
    u.personal_info.location.coordinates.latitude AS latitude,
    u.personal_info.location.coordinates.longitude AS longitude,
    u.personal_info.location.city AS city,
    u.personal_info.location.postal_code AS zip,
    u.personal_info.location.state AS country_state,
    u.tutor.accepts_new_students AS tutor_accepts_new_students,
    u.tutor.max_travel_distance AS tutor_max_travel_distance,
    u.tutor.course_offerings AS tutor_course_offerings,
    u.tutor.can_teach_online AS tutor_can_teach_online,
    u.tutor.has_availability AS tutor_has_availability,
    u.tutor.profile.description AS tutor_long_description,
    u.tutor.onboarding.motivation AS tutor_motivation,
    Cardinality(u.tutor.course_offerings) AS tutor_course_offerings_count,
    Cardinality(u.tutor.course_offerings) > 0 AS tutor_has_course_offerings,
    Cardinality(u.tutor.course_offerings) > 0 AS tutor_has_accepted_course,
    IF(
      tat.tutor IS NOT NULL,
      If(
        tat.accepted_lessons IS NOT NULL,
        tat.accepted_lessons,
        0
      ),
      If(
        tas.accepted_lessons IS NOT NULL,
        tas.accepted_lessons,
        0
      )
    ) as accepted_lessons,
    IF(
      tat.tutor IS NOT NULL,
      IF(
        tat.has_accepted_lesson IS NOT NULL,
        tat.has_accepted_lesson,
        false
      ),
      IF(
        tas.has_accepted_lesson IS NOT NULL,
        tas.has_accepted_lesson,
        false
      )
    ) AS has_accepted_lesson,
    IF(
      tat.tutor IS NOT NULL,
      IF(
        tat.appointment_requests IS NOT NULL,
        tat.appointment_requests,
        0
      ),
      IF(
        tas.appointment_requests IS NOT NULL,
        tas.appointment_requests,
        0
      )
    ) AS appointment_requests,
    IF(
      tat.tutor IS NOT NULL,
      IF(
        tat.has_appointment_request IS NOT NULL,
        tat.has_appointment_request,
        false
      ),
      IF(
        tas.has_appointment_request IS NOT NULL,
        tas.has_appointment_request,
        false
      )
    ) AS has_appointment_request,
    IF(
      tat.tutor IS NOT NULL,
      COALESCE(tat.revenue, 0),
      COALESCE(tas.revenue, 0)
    ) AS revenue,
    IF(
      tat.tutor IS NOT NULL,
      COALESCE(tat.tutorcost, 0),
      COALESCE(tas.tutorcost, 0)
    ) AS tutorcost,
    IF(
      tat.tutor IS NOT NULL,
      COALESCE(tat.revenue - tat.tutorcost, 0),
      COALESCE(tas.revenue - tas.tutorcost, 0)
    ) AS margin,
    IF(
      tat.tutor IS NOT NULL,
      tat.first_lesson_date,
      tas.first_lesson_date
    ) AS first_lesson_date,
    IF(
      tat.tutor IS NOT NULL,
      tat.last_lesson_date,
      tas.last_lesson_date
    ) AS last_lesson_date,
    IF(
      tat.tutor IS NOT NULL,
      tat.first_appointment_request_date,
      tas.first_appointment_request_date
    ) AS first_appointment_request_date,
    IF(
      tat.tutor IS NOT NULL,
      tat.accepted_intro_lessons,
      tas.accepted_intro_lessons
    ) AS accepted_intro_lessons,
    IF(
      tat.tutor IS NOT NULL,
      COALESCE(tat.has_accepted_intro_lesson, false),
      COALESCE(tas.has_accepted_intro_lesson, false)
    ) AS has_accepted_intro_lesson,
    IF(
      tat.tutor IS NOT NULL,
      tat.intro_requests,
      tas.intro_requests
    ) AS intro_requests,
    IF(
      tat.tutor IS NOT NULL,
      COALESCE(tat.has_intro_request, false),
      COALESCE(tas.has_intro_request, false)
    ) AS has_intro_request,
    IF(
      tat.tutor IS NOT NULL,
      tat.first_intro_request_date,
      tas.first_intro_request_date
    ) AS first_intro_request_date,
    IF(
      tat.tutor IS NOT NULL,
      COALESCE(tat.accepted_booking_contacts, 0),
      COALESCE(tas.accepted_booking_contacts, 0)
    ) AS accepted_booking_contacts,
    label_name,
    label_id,
    label_section,
    IF(
      tat.tutor IS NOT NULL,
      first_message_date_tutor,
      first_message_date_student
    ) AS first_message_date,
    is_chat_owner,
    number_of_rooms,
    is_responded,
    up.user_id IS NOT NULL AS has_package,
    COALESCE(up.package_revenue, 0) AS package_revenue,
    COALESCE(up.package_tutor_cost_estimated, 0) AS package_tutor_cost_estimated,
    COALESCE(up.package_margin_estimated, 0) AS package_margin_estimated,
    online_status
  FROM awsdatacatalog.tutoring_user.users u
    LEFT JOIN (
      SELECT ta.tutor,
        --tutoring lesson requests
        sum(cast(ta.type = 'lesson' AS integer)) AS appointment_requests,
        sum(cast(ta.type = 'lesson' AS integer)) > 0 AS has_appointment_request,
        min(IF(ta.type = 'lesson', ta.created_at, NULL)) AS first_appointment_request_date,
        -- accepted tutoring lessons
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          )
        ) AS accepted_lessons,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          )
        ) > 0 AS has_accepted_lesson,
        min(
          IF(
            ta.status = 'accepted'
            AND ta.type = 'lesson',
            ta.BEGIN,
            NULL
          )
        ) AS first_lesson_date,
        max(
          IF(
            ta.status = 'accepted'
            AND ta.type = 'lesson',
            ta.BEGIN,
            NULL
          )
        ) AS last_lesson_date,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          ) * cast(
            IF(amount_student_unit = 1, amount_student, 0) AS decimal(10, 2)
          )
        ) AS revenue,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          ) * cast(
            IF(amount_student_unit = 1, amount_student, 0) AS decimal(10, 2)
          )
        ) AS revenue_single_lessons,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          ) * cast(
            IF(amount_student_unit = 2, amount_student, 0) AS decimal(10, 2)
          )
        ) AS package_credits_used,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          ) * cast(amount_tutor AS decimal(10, 2))
        ) AS tutorcost,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          ) * cast(
            IF(amount_student_unit = 1, amount_tutor, 0) AS decimal(10, 2)
          )
        ) AS tutor_cost_single_lesson,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          ) * cast(
            IF(amount_student_unit = 2, amount_tutor, 0) AS decimal(10, 2)
          )
        ) AS tutor_cost_package,
        -- intro appointments
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'intro' AS integer
          )
        ) AS accepted_intro_lessons,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'intro' AS integer
          )
        ) > 0 AS has_accepted_intro_lesson,
        sum(cast(ta.type = 'intro' AS integer)) AS intro_requests,
        sum(cast(ta.type = 'intro' AS integer)) > 0 AS has_intro_request,
        min(IF(ta.type = 'intro', ta.created_at, NULL)) AS first_intro_request_date,
        --
        count(
          DISTINCT IF(
            ta.status = 'accepted'
            AND ta.type = 'lesson',
            student,
            NULL
          )
        ) AS accepted_booking_contacts
      FROM awsdatacatalog.tutoring_booking.appointments AS ta
        LEFT JOIN snapshot s ON true
      WHERE ta.snapshot_id = s.id
      GROUP BY ta.tutor
    ) AS tat ON tat.tutor = u._id
    LEFT JOIN (
      SELECT ta.student,
        --tutoring lesson requests
        sum(cast(ta.type = 'lesson' AS integer)) AS appointment_requests,
        sum(cast(ta.type = 'lesson' AS integer)) > 0 AS has_appointment_request,
        min(IF(ta.type = 'lesson', ta.created_at, NULL)) AS first_appointment_request_date,
        -- accepted tutoring lessons
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          )
        ) AS accepted_lessons,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          )
        ) > 0 AS has_accepted_lesson,
        min(
          IF(
            ta.status = 'accepted'
            AND ta.type = 'lesson',
            ta.BEGIN,
            NULL
          )
        ) AS first_lesson_date,
        max(
          IF(
            ta.status = 'accepted'
            AND ta.type = 'lesson',
            ta.BEGIN,
            NULL
          )
        ) AS last_lesson_date,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          ) * cast(
            IF(amount_student_unit = 1, amount_student, 0) AS decimal(10, 2)
          )
        ) AS revenue,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          ) * cast(
            IF(amount_student_unit = 1, amount_student, 0) AS decimal(10, 2)
          )
        ) AS revenue_single_lesson,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          ) * cast(
            IF(amount_student_unit = 2, amount_student, 0) AS decimal(10, 2)
          )
        ) AS package_credits_used,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          ) * cast(amount_tutor AS decimal(10, 2))
        ) AS tutorcost,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          ) * cast(
            IF(amount_student_unit = 1, amount_tutor, 0) AS decimal(10, 2)
          )
        ) AS tutor_cost_single_lesson,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'lesson' AS integer
          ) * cast(
            IF(amount_student_unit = 2, amount_tutor, 0) AS decimal(10, 2)
          )
        ) AS tutor_cost_package,
        -- intro appointments
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'intro' AS integer
          )
        ) AS accepted_intro_lessons,
        sum(
          cast(
            ta.status = 'accepted'
            AND ta.type = 'intro' AS integer
          )
        ) > 0 AS has_accepted_intro_lesson,
        sum(cast(ta.type = 'intro' AS integer)) AS intro_requests,
        sum(cast(ta.type = 'intro' AS integer)) > 0 AS has_intro_request,
        min(IF(ta.type = 'intro', ta.created_at, NULL)) AS first_intro_request_date,
        --
        count(
          DISTINCT IF(
            ta.status = 'accepted'
            AND ta.type = 'lesson',
            tutor,
            NULL
          )
        ) AS accepted_booking_contacts
      FROM awsdatacatalog.tutoring_booking.appointments AS ta
        LEFT JOIN snapshot s ON true
      WHERE ta.snapshot_id = s.id
      GROUP BY ta.student
    ) AS tas ON tas.student = u._id
    LEFT JOIN (
      SELECT l.NAME AS label_name,
        l.id AS label_id,
        l.section AS label_section
      FROM awsdatacatalog.tutoring_api.labels AS l
        LEFT JOIN snapshot s ON true
      WHERE l.snapshot_id = s.id
    ) l ON u.admin_label.label_id = l.label_id
    LEFT JOIN (
      SELECT user_id,
        min(first_message_date_student) AS first_message_date_student,
        min(first_message_date_tutor) AS first_message_date_tutor,
        count(owner) AS is_chat_owner,
        count(DISTINCT ru.room_id) AS number_of_rooms,
        count(participants) AS chats_with_message,
        count(participants > 1) AS is_responded
      FROM awsdatacatalog.tutoring_message.room_users AS ru
        LEFT JOIN snapshot s ON true
        LEFT JOIN (
          SELECT room_id,
            min(IF(role = 'Student', created_at, NULL)) AS first_message_date_student,
            min(IF(role = 'Tutor', created_at, NULL)) AS first_message_date_tutor,
            count(DISTINCT m.user_id) AS participants
          FROM awsdatacatalog.tutoring_message.messages AS m
            LEFT JOIN snapshot s ON true
            LEFT JOIN (
              SELECT _id AS user_id,
                role
              FROM awsdatacatalog.tutoring_user.users AS u
            ) AS u ON m.user_id = u.user_id
          WHERE m.user_id IS NOT NULL
            AND m.snapshot_id = s.id
          GROUP BY room_id
        ) AS participants ON participants.room_id = ru.room_id
      WHERE ru.snapshot_id = s.id
      GROUP BY user_id
    ) AS rooms ON rooms.user_id = u._id
    LEFT JOIN (
      SELECT DISTINCT user_id,
        price AS package_revenue,
        cast(
          1.00 * before_price * 0.57 AS decimal(10, 2)
        ) AS package_tutor_cost_estimated,
        1.00 * price - cast(1.00 * regular_price * 0.57 AS decimal(10, 2)) AS package_margin_estimated
      FROM awsdatacatalog.tutoring_booking.user_packages AS up
        LEFT JOIN snapshot s ON true
      WHERE deleted_at IS NULL
        AND status = 2 --(active)
        AND up.snapshot_id = s.id
    ) AS up ON u._id = up.user_id
    LEFT JOIN (
      SELECT _id AS heard_about_id,
        IF(kind = 'Other', 'Other', text) AS heard_about_text,
        IF(kind = 'Other', text, NULL) AS heard_about_other,
        role AS heard_about_role
      FROM awsdatacatalog.tutoring_user.heard_about
    ) AS heard_about_table ON heard_about_table.heard_about_id = u.heard_about
    LEFT JOIN (
      WITH temp AS (
        SELECT id,
          student,
          tutor,
          status,
          online
        FROM tutoring_booking.appointments
        WHERE hidden_student != 1
          OR hidden_tutor != 1
      ),
      c AS (
        SELECT DISTINCT temp.student AS student_a,
          temp.tutor AS tutor_a,
          temp.online AS online_a,
          t.student AS student_b,
          t.tutor AS tutor_b,
          t.online AS online_b
        FROM temp
          LEFT JOIN temp AS t ON temp.student = t.student
      )
      SELECT DISTINCT student,
        CASE
          WHEN student IN (
            SELECT DISTINCT student_a
            FROM c
            WHERE student_a = student_b
              AND tutor_a != tutor_b
              AND online_a != online_b
          ) THEN 2
          WHEN student NOT IN (
            SELECT DISTINCT student_a
            FROM c
            WHERE student_a = student_b
              AND tutor_a != tutor_b
              AND online_a != online_b
          )
          AND online = 0 THEN 0
          WHEN student NOT IN (
            SELECT DISTINCT student_a
            FROM c
            WHERE student_a = student_b
              AND tutor_a != tutor_b
              AND online_a != online_b
          )
          AND online = 1 THEN 1
        END AS online_status
      FROM tutoring_booking.appointments
    ) AS status ON status.student = u._id
  WHERE (
      (
        u.is_test = false
        OR u.is_test IS NULL
      )
    )
)
SELECT DISTINCT _id AS student_id,
  registration_date,
  last_active_at,
  city,
  country,
  state,
  online_status AS has_online_lessons,
  has_package,
  latitude,
  longitude
FROM user_table
WHERE role = 'Student' AND country = 'nl';
