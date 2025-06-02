select * from (
SELECT main.*, meta.step_code, meta.DATASALON_PRODUCT_TITLE
FROM (
  WITH submission_cte AS (
    SELECT 
      platform_site_manuscript_id,
      submissionidoriginal,
      submissiondateoriginal,
      pubdnumber,
      datasalon_code,
      accepted_paper,
      decisiontype,
      CASE
    WHEN accepted_paper = 1 THEN 'accept'
    WHEN rejected_paper = 1 THEN 'rejected'
    WHEN (accepted_paper = 0 OR accepted_paper IS NULL)
         AND (rejected_paper = 0 OR rejected_paper IS NULL) THEN 'under review'
  END AS paper_status,
      lead_author_institute,
      lead_author_first_name,
      lead_author_last_name,
      lead_author_department,
      lead_author_first_name || ' ' || lead_author_last_name AS lead_author_name,
    CASE 
    WHEN EXTRACT(MONTH FROM decisiondate) >= 5 
    THEN TO_DATE(EXTRACT(YEAR FROM decisiondate) + 1 || '-01-05', 'YYYY-MM-DD')
    ELSE TO_DATE(EXTRACT(YEAR FROM decisiondate) || '-01-05', 'YYYY-MM-DD')
END AS fiscal_year
    FROM 
      PROD_EDW.RESEARCH.SUBMISSIONS_ONE_ROW_PER_MANUSCRIPT

   //  WHERE  accepted_paper = 1    we want to include all the papers
  ),

  reviewer_cte AS (
    SELECT 
      platform_site_manuscript_id,
      LISTAGG(DISTINCT REVIEWERPRIMARYEMAILADDRESS, ' / ') 
        WITHIN GROUP (ORDER BY REVIEWERPRIMARYEMAILADDRESS) AS reviewer_primary_email,
      
      LISTAGG(DISTINCT reviewerfullname, ' / ') 
        WITHIN GROUP (ORDER BY reviewerfullname) AS reviewer_names,

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
  //  WHERE scoresheetcompleteddate IS NOT NULL
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
)


  

  SELECT 
    s.datasalon_code,
    s.platform_site_manuscript_id,
    s.submissionidoriginal,
    s.submissiondateoriginal,
    s.pubdnumber,
    s.decisiontype,
    s.accepted_paper,
    s.paper_status,
    s.lead_author_institute,
    s.lead_author_department,
    s.lead_author_name,
    s.fiscal_year,
    a.author_names,
    a.author_primary_email,
    r.reviewer_names,
    r.reviewer_primary_email,
    r.orchid_ids,
    r.reviewerperson_ids,
    r.reviewer_completed as no_of_reviewer_submitted,
    d.institutions as reviewer_institution,
    d.departments as reviewer_department,

    -- Name match flag
/* CASE 
  WHEN s.lead_author_first_name IS NOT NULL 
       AND s.lead_author_last_name IS NOT NULL
       AND LOWER(TRIM(reviewerfirstname)) = LOWER(TRIM(s.lead_author_first_name))
       AND LOWER(TRIM(reviewerlastname)) = LOWER(TRIM(s.lead_author_last_name))
  THEN 1 ELSE 0 
END AS name_flag, */

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


-- Email Flag

CASE 
  WHEN author_primary_email IS NOT NULL 
  AND reviewer_primary_email IS NOT NULL 
  AND author_primary_email ILIKE '%' || NULLIF(reviewer_primary_email, '') || '%' 
  THEN 1 
  ELSE 0 
END AS email_flag,



    -- Institution match flag
    CASE 
      WHEN s.lead_author_institute IS NOT NULL 
           AND d.institutions ILIKE '%' || s.lead_author_institute || '%' 
      THEN 1 ELSE 0 
    END AS institution_flag,

    -- Department match flag only if institution matched
    CASE 
      WHEN s.lead_author_institute IS NOT NULL 
           AND d.institutions ILIKE '%' || s.lead_author_institute || '%' 
           AND s.lead_author_department IS NOT NULL 
           AND d.departments ILIKE '%' || s.lead_author_department || '%' 
      THEN 1 ELSE 0 
    END AS department_flag

  FROM 
    submission_cte s
  LEFT JOIN 
    reviewer_cte r ON s.platform_site_manuscript_id = r.platform_site_manuscript_id
  LEFT JOIN 
    department_cte d ON s.platform_site_manuscript_id = d.platform_site_manuscript_id
  LEFT JOIN 
    author_email_cte a ON s.platform_site_manuscript_id = a.platform_site_manuscript_id    
) main 

LEFT JOIN (
  SELECT DISTINCT
    datasalon_code, 
    step_code ,
    DATASALON_PRODUCT_TITLE
  FROM 
    PROD_EDW.RESEARCH_ANALYTICS.DIM_T_PRODUCT_METADATA
) meta 
  ON main.datasalon_code = meta.datasalon_code   




WHERE 
  main.datasalon_code NOT LIKE '97%'
  AND main.datasalon_code NOT LIKE 'S1%'
and fiscal_year >= '2020-01-01'
ORDER BY 
fiscal_year desc

) where

name_flag = 1 
or email_flag = 1 
OR institution_flag = 1 
OR department_flag = 1
