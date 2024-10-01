CREATE OR REPLACE PROCEDURE DHWAMAZONKP.GenerateDimTime (
  -- Input parameters: Start Date and End Date
  startdate DATE DEFAULT TO_DATE('2012-01-01', 'YYYY-MM-DD'),
  enddate DATE DEFAULT TO_DATE('2020-12-31', 'YYYY-MM-DD')
) AS
BEGIN
    DECLARE
        i number := 0;
        v_date DATE;
        v_rowcount int := 0;
        
        dk NUMBER;
        yn NUMBER;
        Qn NUMBER;
        smn NUMBER;
        mno NUMBER;
        wkn NUMBER;
        dn NUMBER;
        mn VARCHAR2(20);
        Tmn VARCHAR2(20);
    BEGIN
        execute immediate 'TRUNCATE TABLE DWHAMAZONKP.DimTimeKP';

        loop
            v_date := startdate + i;
            
            dk := TO_NUMBER(TO_CHAR(v_date, 'yyyymmdd'));
            yn := TO_NUMBER(TO_CHAR(v_date, 'YYYY'));
            Qn := TO_NUMBER(TO_CHAR(v_date, 'Q'));
            smn := CASE WHEN Qn > 2 THEN 2 ELSE 1 END;
            mno := TO_NUMBER(TO_CHAR(v_date, 'MM'));
            wkn := TO_NUMBER(TO_CHAR(v_date, 'WW'));
            dn := TO_NUMBER(TO_CHAR(v_date, 'DD'));
            mn := rtrim(TO_CHAR(v_date, 'Month'));
            Tmn := rtrim(
              CASE
                WHEN mno = 1 THEN 'Styczen'
                WHEN mno = 2 THEN 'Luty'
                WHEN mno = 3 THEN 'Marzec'
                WHEN mno = 4 THEN 'Kwiecien'
                WHEN mno = 5 THEN 'Maj'
                WHEN mno = 6 THEN 'Czerwiec'
                WHEN mno = 7 THEN 'Lipiec'
                WHEN mno = 8 THEN 'Sierpien'
                WHEN mno = 9 THEN 'Wrzesien'
                WHEN mno = 10 THEN 'Pazdziernik'
                WHEN mno = 11 THEN 'Listopad'
                ELSE 'Grudzien'
              END);
              
             INSERT INTO DimTimeKP VALUES (dk, v_date, yn, smn, Qn, mno, wkn, dn, mn, Tmn);
             commit;

            v_rowcount := v_rowcount + 1;
            i := i + 1;
            exit when startdate + i > enddate;
        end loop;

        dbms_output.put_line('Loaded ' || v_rowcount || 'records');
    END;
END GenerateDimTime;