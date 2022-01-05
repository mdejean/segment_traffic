drop table if exists segment_traffic;
create table segment_traffic (
    name varchar,
    "from" varchar,
    "to" varchar,
    dhv int,
    ddhv int,
    aadt int,
    federal_direction int,
    functional_classification int,
    street varchar,
    trafdir varchar,
    number_travel_lanes int,
    number_total_lanes int,
    streetwidth_min float,
    shape_length float,
    rcsta int,
    segmentid varchar,
    dist float,
    dist_stddev float,
    vmt float,
    number_roadbeds int,
    vmt_allocated float,
    dhv_per_ft float,
    aadt_per_ft float
);
SELECT AddGeometryColumn ('public','segment_traffic','geom',2263,'MultiCurve',2);

--with lion_clean as (
drop table if exists lion_clean;
create table lion_clean as 
    select
        lw.*
    from lion lw
    -- exclude name/address duplicates
    left join (
        select 
            objectid, 
            row_number() over (partition by segmentid order by street) n
        from lion l 
        where rb_layer in ('R', 'B') 
        and specaddr = ' ' 
        and segmenttyp not in ('F', 'C', 'T')
        and l.trafdir in ('W', 'A', 'T')
        and "segcount"::integer > 1
    ) dup on dup.objectid = lw.objectid and dup.n > 1
    where dup.n is null
    -- generic segments
    and rb_layer in ('R', 'B') 
    -- special address entries
    and specaddr = ' ' 
    -- roadbed connector segments
    and segmenttyp not in ('F', 'C', 'T')
    -- non vehicular segments
    and trafdir in ('W', 'A', 'T');
create index ix_lion_clean_geom on lion_clean using gist(geom);
create index ix_lion_clean_streetcode on lion_clean(streetcode);
create index ix_lion_clean_segmentid on lion_clean(segmentid);
--)
insert into segment_traffic
select
    r.name,
    r."from",
    r."to",
    tc.dhv, 
    tc.ddhv, 
    coalesce(tc.aadt, tc.aadt_last_act) aadt, 
    tc.federal_direction,
    tc.fc as functional_classification,
    l.street, 
    l.trafdir,
    all_roadbeds.number_travel_lanes, 
    all_roadbeds.number_total_lanes, 
    all_roadbeds.streetwidth_min,
    l.shape_length,
    rs.rcsta, 
    rs.segmentid, 
    rs.dist,
    rs.dist_stddev,
    tc.aadt * 365 * l.shape_length / 5280 as vmt,
    all_roadbeds.number_roadbeds, 
    tc.aadt * 365 * l.shape_length / 5280 / nullif(all_roadbeds.number_roadbeds,0) as vmt_allocated,
    tc.dhv / nullif(all_roadbeds.streetwidth_min, 0) as dhv_per_ft,
    tc.aadt / nullif(all_roadbeds.streetwidth_min, 0) as aadt_per_ft,
    l.geom
from (
    select 
        l.segmentid, 
        s.rcsta, 
        s.dist,
        s.dist_stddev
    from lion_clean l
    left join lateral (
        select 
            t.rcsta,
            dist,
            dist_stddev
        from tdv_geom t
        --( -- tdv contains many duplicate segments
            -- select 
                -- ST_Union(geom) geom,
                -- max(dhv) dhv,
                -- max(aadt) aadt,
                -- rcsta
            -- from tdv
            -- group by rcsta
        -- ) t
        left join roadway r on r.rcsta = t.rcsta
        join lateral ( -- average distance from the (small) lion segment's points to the (large) tdv segment
            select 
                avg(ST_Distance(t.geom, dp.geom)) dist,
                stddev(ST_Distance(t.geom, dp.geom)) dist_stddev
            from ( select (ST_DumpPoints(l.geom)).* ) dp
        ) s2 on true
        where ST_DWithin(t.geom, l.geom, 500)
        and (
            --it is for this street
            r.streetcode = l.streetcode
            -- or it's not found in roadway (ramps?)
            or r.rcsta is null
            -- or it didn't code to any street
            or r.streetcode is null
        ) and (
            dist < 0.2 * l.shape_length
            or ( --parallel segments
                dist < 5 * l.streetwidth_min
                and dist_stddev < 0.1 * l.shape_length
            )
        )
        order by dist, coalesce(t.dhv, 0) desc, coalesce(t.aadt, 0) desc
        limit 1
    ) s on true
) rs
join lion_clean l on rs.segmentid = l.segmentid
left join lateral (
    select true as v
    from all_roadbeds rp
    join all_roadbeds rp2 on rp.generic = rp2.generic
    join lion_clean l2 on rp2.roadbed = l2.segmentid
    where
        rp.roadbed = l.segmentid
    and
        l2.trafdir = 'T'
    limit 1
) has_two_way_roadbed on true
left join lateral (
    select
        count(*) number_roadbeds,
        sum(l2.streetwidth_min) streetwidth_min,
        sum(l2.number_travel_lanes) number_travel_lanes,
        sum(l2.number_total_lanes) number_total_lanes
    from all_roadbeds rp
    -- (select generic, roadbed from roadbed_pointer
        -- union all
        -- select generic, generic from roadbed_pointer
        -- where segmenttype = 'B'
        -- union
        -- select l.segmentid, l.segmentid
        -- from lion l where l.rb_layer = 'B') rp
    join all_roadbeds rp2 on rp.generic = rp2.generic
    join lion_clean l2 on rp2.roadbed = l2.segmentid and l2.specaddr = ' '
    where rp.roadbed = l.segmentid
    and (
        l2.trafdir = l.trafdir
        or has_two_way_roadbed.v is not null
    )
) all_roadbeds on true
join (select --direction of street overall
    streetcode,
    sum(xto - xfrom) dx,
    sum(yto - yfrom) dy
    from lion_clean
    group by streetcode
) s on l.streetcode = s.streetcode
join lateral (
    select 90 - atan2d(dy, dx) - case when l.lboro = 1 then 30 else 0 end azimuth
) s2 on true
join lateral (
    select case when l.trafdir = 'A' then 180 + azimuth else azimuth end traf_azimuth
) s3 on true
join lateral (
    select case 
        when traf_azimuth between -45 and 45 then 1 
        when traf_azimuth between 45 and 135 then 3
        when traf_azimuth between 135 and 225 or traf_azimuth between -225 and -135 then 5
        when traf_azimuth between 225 and 315 or traf_azimuth between -135 and -45 then 7
    end as traf_federal_dir
) s4 on true
left join traffic_count tc 
    on rs.rcsta = tc.rcsta 
    and (
        (tc.full_count and has_two_way_roadbed.v is not null) -- two way segments get two way counts
        or (
            tc.full_count is null 
            and has_two_way_roadbed.v is null
            and (  (tc.federal_direction in (1, 3) and traf_federal_dir in (1, 3)) 
                or (tc.federal_direction in (5, 7) and traf_federal_dir in (5, 7))
            ) -- one way segments 
        )
    )
left join lateral (
    select 
        *
    from roadway r2
    where rs.rcsta = r2.rcsta
    order by coalesce(r2.length, 0) desc
    limit 1
) r on true;

create index ix_segment_traffic_geom on segment_traffic using gist(geom);

-- delete a one way count for each segment but only if there's another segment going the opposite direction

--delete from segment_traffic st where case when azimuth  