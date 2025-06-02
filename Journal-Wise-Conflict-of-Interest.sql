
SELECT main.*, meta.step_code, meta.DATASALON_PRODUCT_TITLE
FROM (
  WITH submission_cte AS (
    SELECT 
      platform_site_manuscript_id,
      datasalon_code,
      accepted_paper,
      rejected_paper,
      lead_author_institute,
      lead_author_department,
      lead_author_first_name,
      lead_author_last_name,
      lead_author_first_name || ' ' || lead_author_last_name AS lead_author_name,
    CASE 
    WHEN EXTRACT(MONTH FROM decisiondate) >= 5 
    THEN TO_DATE(EXTRACT(YEAR FROM decisiondate) + 1 || '-01-05', 'YYYY-MM-DD')
    ELSE TO_DATE(EXTRACT(YEAR FROM decisiondate) || '-01-05', 'YYYY-MM-DD')
END AS fiscal_year,


   
    FROM 
      PROD_EDW.RESEARCH.SUBMISSIONS_ONE_ROW_PER_MANUSCRIPT
     
// WHERE  accepted_paper = 1    we want to include al the submission types
  ),

  reviewer_cte AS (
    SELECT 
      platform_site_manuscript_id,
      LISTAGG(DISTINCT reviewerfullname, ' / ') 
        WITHIN GROUP (ORDER BY reviewerfullname) AS reviewer_names,

        LISTAGG(DISTINCT REVIEWERPRIMARYEMAILADDRESS, ' / ') 
        WITHIN GROUP (ORDER BY REVIEWERPRIMARYEMAILADDRESS) AS reviewer_primary_email,

      LISTAGG(DISTINCT REVIEWERFIRSTNAME  , ' / ') 
      WITHIN GROUP (ORDER BY REVIEWERFIRSTNAME) AS REVIEWERFIRSTNAME,

      LISTAGG(DISTINCT REVIEWERLASTNAME, ' / ') 
      WITHIN GROUP (ORDER BY REVIEWERLASTNAME) AS REVIEWERLASTNAME,
        
      LISTAGG(DISTINCT reviewerorcidid, ' / ') 
        WITHIN GROUP (ORDER BY reviewerorcidid) AS orchid_ids,
      LISTAGG(DISTINCT reviewerpersonid, ' / ') 
        WITHIN GROUP (ORDER BY reviewerpersonid) AS reviewerperson_ids,
      COUNT(DISTINCT scoresheetcompleteddate) AS reviewer_completed
    FROM 
      PROD_EDW.SBM.S1_REVIEWERS
   // WHERE scoresheetcompleteddate IS NOT NULL    we want to include all the reviewers
    GROUP BY 
      platform_site_manuscript_id
  ),

  department_cte AS (
    SELECT 
      platform_site_manuscript_id,
      LISTAGG(DISTINCT institution, ' / ') 
        WITHIN GROUP (ORDER BY institution) AS institutions,
      LISTAGG(DISTINCT department, ' / ') 
        WITHIN GROUP (ORDER BY department) AS departments
    FROM 
      PROD_EDW.SBM.S1_REVIEWERS_DEPARTMENT
    GROUP BY 
      platform_site_manuscript_id
  ),

author_email_cte AS (
  SELECT 
    platform_site_manuscript_id,
     LISTAGG(DISTINCT LOWER(TRIM(AUTHORFULLNAME)), ' / ') 
      WITHIN GROUP (ORDER BY LOWER(TRIM(AUTHORFULLNAME))) AS author_names,
    LISTAGG(DISTINCT LOWER(TRIM(AUTHORPRIMARYEMAILADDRESS)), ' / ') 
      WITHIN GROUP (ORDER BY LOWER(TRIM(AUTHORPRIMARYEMAILADDRESS))) AS author_primary_email
  FROM 
    PROD_EDW.SBM.S1_AUTHORS
  GROUP BY 
    platform_site_manuscript_id
),

  flagged_data AS (
    SELECT 
      s.datasalon_code,
      s.platform_site_manuscript_id,
      s.fiscal_year,
      s.accepted_paper,
      s.rejected_paper,
     

      -- Flags
CASE 
  WHEN (
        author_names IS NOT NULL 
        AND reviewer_names IS NOT NULL 
        AND reviewer_names <> '' 
        AND author_names ILIKE '%' || reviewer_names || '%'
       )
    OR (
        s.lead_author_first_name IS NOT NULL 
        AND s.lead_author_last_name IS NOT NULL
        AND LOWER(TRIM(reviewerfirstname)) = LOWER(TRIM(s.lead_author_first_name))
        AND LOWER(TRIM(reviewerlastname)) = LOWER(TRIM(s.lead_author_last_name))
       )
  THEN 1 
  ELSE 0 
END AS name_flag,

CASE 
  WHEN author_primary_email IS NOT NULL 
  AND reviewer_primary_email IS NOT NULL 
  AND author_primary_email ILIKE '%' || NULLIF(reviewer_primary_email, '') || '%' 
  THEN 1 
  ELSE 0 
END AS email_flag,

      CASE 
        WHEN s.lead_author_institute IS NOT NULL 
             AND d.institutions ILIKE '%' || s.lead_author_institute || '%' 
        THEN 1 ELSE 0 
      END AS institute_flag,

      CASE 
        WHEN s.lead_author_institute IS NOT NULL 
             AND d.institutions ILIKE '%' || s.lead_author_institute || '%' 
             AND s.lead_author_department IS NOT NULL 
             AND d.departments ILIKE '%' || s.lead_author_department || '%' 
        THEN 1 ELSE 0 
      END AS department_flag,

      -- Combined flag
CASE 
  WHEN 
    -- Name match
    (s.lead_author_first_name IS NOT NULL AND s.lead_author_last_name IS NOT NULL 
     AND LOWER(TRIM(r.reviewerfirstname)) = LOWER(TRIM(s.lead_author_first_name)) 
     AND LOWER(TRIM(r.reviewerlastname)) = LOWER(TRIM(s.lead_author_last_name)))

    -- OR Email match
    OR (a.author_primary_email IS NOT NULL AND r.reviewer_primary_email IS NOT NULL 
        AND a.author_primary_email ILIKE '%' || r.reviewer_primary_email || '%')

    -- OR Institution match
    OR (s.lead_author_institute IS NOT NULL AND d.institutions ILIKE '%' || s.lead_author_institute || '%')

    -- OR Department match (only if institution matched)
    OR (
      s.lead_author_institute IS NOT NULL AND d.institutions ILIKE '%' || s.lead_author_institute || '%'
      AND s.lead_author_department IS NOT NULL AND d.departments ILIKE '%' || s.lead_author_department || '%'
    )
  THEN 1 ELSE 0
END AS total_flag









      

    FROM 
      submission_cte s
    LEFT JOIN 
      reviewer_cte r ON s.platform_site_manuscript_id = r.platform_site_manuscript_id
    LEFT JOIN 
      department_cte d ON s.platform_site_manuscript_id = d.platform_site_manuscript_id
      LEFT JOIN 
    author_email_cte a ON s.platform_site_manuscript_id = a.platform_site_manuscript_id
  )

  -- Aggregate per journal
  SELECT 
    datasalon_code,
    COUNT(*) AS total_manuscripts,
    SUM(name_flag) AS name_match_count,
    SUM(email_flag) as email_match_count,
    SUM(institute_flag) AS institute_match_count,
    SUM(department_flag) AS department_match_count,
    SUM(total_flag) AS total_match_count,
    fiscal_year,
    COUNT(CASE WHEN accepted_paper = 1 THEN 1 END) AS accepted_count,
    COUNT(CASE WHEN rejected_paper = 1 THEN 1 END) AS rejected_count,
    COUNT(CASE 
           WHEN (accepted_paper = 0 OR accepted_paper IS NULL) 
             AND (rejected_paper = 0 OR rejected_paper IS NULL) 
           THEN 1 
   END) AS revision_count
   
  FROM 
    flagged_data
  GROUP BY 
    datasalon_code, fiscal_year
  ORDER BY 
    total_match_count DESC

) main 

LEFT JOIN (
  SELECT DISTINCT datasalon_code, step_code, DATASALON_PRODUCT_TITLE
  FROM PROD_EDW.RESEARCH_ANALYTICS.DIM_T_PRODUCT_METADATA
) meta 
  ON main.datasalon_code = meta.datasalon_code

WHERE 
  main.datasalon_code NOT LIKE '97%'
  AND main.datasalon_code NOT LIKE 'S1%'
 and total_match_count != 0
 and fiscal_year >= '2020-01-01'


ORDER BY 
  total_manuscripts DESC

