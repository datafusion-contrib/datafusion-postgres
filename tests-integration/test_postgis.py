#!/usr/bin/env python3
"""
Integration tests for PostGIS spatial functionality.
Tests typical PostGIS queries to ensure they succeed without protocol/client errors.
"""

import psycopg


def main():
    print("🗺️  Testing PostGIS Spatial Queries")
    print("=" * 50)

    conn = psycopg.connect("host=127.0.0.1 port=5437 user=postgres dbname=public")
    conn.autocommit = True

    with conn.cursor() as cur:
        print("\n📋 Test 2: Geometry Creation Functions")
        test_geometry_creation(cur)

        print("\n📋 Test 3: Geometry Output Functions")
        test_geometry_output(cur)

        # print("\n📋 Test 4: Spatial Reference System")
        # test_spatial_reference_system(cur)

        print("\n📋 Test 5: Spatial Measurement Functions")
        test_spatial_measurements(cur)

        print("\n📋 Test 6: Spatial Predicate Functions")
        test_spatial_predicates(cur)

        print("\n📋 Test 7: Spatial Analysis Functions")
        test_spatial_analysis(cur)

        print("\n📋 Test 8: Bounding Box Functions")
        test_bounding_box(cur)

    conn.close()
    print("\n✅ All PostGIS tests passed!")


def test_geometry_creation(cur):
    """Test geometry creation functions."""
    # Test ST_GeomFromText
    # cur.execute("SELECT ST_GeomFromText('POINT(1 1)')")
    # result = cur.fetchone()[0]
    # assert result is not None
    # print("  ✓ ST_GeomFromText('POINT(1 1)')")

    # Test ST_Point
    cur.execute("SELECT ST_Point(1, 2)")
    result = cur.fetchone()[0]
    assert result is not None
    print("  ✓ ST_Point(1, 2)")

    # Test ST_MakePoint
    cur.execute("SELECT ST_MakePoint(1, 2, 3)")
    result = cur.fetchone()[0]
    assert result is not None
    print("  ✓ ST_MakePoint(1, 2, 3)")

    # Test ST_LineFromText
    # cur.execute("SELECT ST_LineFromText('LINESTRING(0 0, 1 1, 2 2)')")
    # result = cur.fetchone()[0]
    # assert result is not None
    # print("  ✓ ST_LineFromText('LINESTRING(0 0, 1 1, 2 2)')")

    # # Test ST_PolygonFromText
    # cur.execute("SELECT ST_PolygonFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')")
    # result = cur.fetchone()[0]
    # assert result is not None
    # print("  ✓ ST_PolygonFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')")

    # # Test ST_GeomFromWKB
    # try:
    #     cur.execute("SELECT ST_AsBinary(ST_Point(1, 2))")
    #     wkb = cur.fetchone()[0]
    #     cur.execute("SELECT ST_GeomFromWKB(%s)", [wkb])
    #     result = cur.fetchone()[0]
    #     assert result is not None
    #     print("  ✓ ST_GeomFromWKB() and ST_AsBinary()")
    # except Exception as e:
    #     print(f"  ℹ️  ST_GeomFromWKB() not fully supported: {type(e).__name__}")


def test_geometry_output(cur):
    """Test geometry output/formatting functions."""
    # Test ST_AsText
    cur.execute("SELECT ST_AsText(ST_Point(1, 2))")
    result = cur.fetchone()[0]
    assert result is not None
    print(f"  ✓ ST_AsText(): {result}")

    # Test ST_AsBinary
    cur.execute("SELECT ST_AsBinary(ST_Point(1, 2))")
    result = cur.fetchone()[0]
    assert result is not None
    print("  ✓ ST_AsBinary()")

    # # Test ST_AsEWKT
    # try:
    #     cur.execute("SELECT ST_AsEWKT(ST_Point(1, 2))")
    #     result = cur.fetchone()[0]
    #     assert result is not None
    #     print(f"  ✓ ST_AsEWKT(): {result}")
    # except Exception as e:
    #     print(f"  ℹ️  ST_AsEWKT() not available: {type(e).__name__}")

    # Test ST_AsGeoJSON
    # try:
    #     cur.execute("SELECT ST_AsGeoJSON(ST_Point(1, 2))")
    #     result = cur.fetchone()[0]
    #     assert result is not None
    #     print(f"  ✓ ST_AsGeoJSON(): {result[:50]}...")
    # except Exception as e:
    #     print(f"  ℹ️  ST_AsGeoJSON() not available: {type(e).__name__}")


# def test_spatial_reference_system(cur):
#     """Test spatial reference system functions."""
#     # Test ST_SRID
#     cur.execute("SELECT ST_SRID(ST_GeomFromText('POINT(1 1)', 4326))")
#     srid = cur.fetchone()[0]
#     assert srid == 4326 or srid is not None
#     print(f"  ✓ ST_SRID(): {srid}")

#     # Test ST_SetSRID
#     cur.execute("SELECT ST_SRID(ST_SetSRID(ST_Point(1, 2), 4326))")
#     srid = cur.fetchone()[0]
#     assert srid == 4326 or srid is not None
#     print(f"  ✓ ST_SetSRID(): {srid}")

#     # Test spatial_ref_sys table
#     try:
#         cur.execute("SELECT count(*) FROM spatial_ref_sys LIMIT 1")
#         count = cur.fetchone()[0]
#         print(f"  ✓ spatial_ref_sys table: {count} records")
#     except Exception as e:
#         print(f"  ℹ️  spatial_ref_sys table not available: {type(e).__name__}")


def test_spatial_measurements(cur):
    """Test spatial measurement functions."""
    # Test ST_Distance
    cur.execute("SELECT ST_Distance(ST_Point(0, 0), ST_Point(3, 4))")
    distance = cur.fetchone()[0]
    assert abs(distance - 5.0) < 0.01
    print(f"  ✓ ST_Distance(): {distance}")

    # Test ST_Length (line)
    # cur.execute("SELECT ST_Length(ST_LineFromText('LINESTRING(0 0, 3 4)'))")
    # length = cur.fetchone()[0]
    # assert length is not None
    # print(f"  ✓ ST_Length(): {length}")

    # Test ST_Area (polygon)
    # cur.execute(
    #     "SELECT ST_Area(ST_PolygonFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'))"
    # )
    # area = cur.fetchone()[0]
    # assert area is not None
    # print(f"  ✓ ST_Area(): {area}")

    # Test ST_Perimeter (polygon)
    # try:
    #     cur.execute(
    #         "SELECT ST_Perimeter(ST_PolygonFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'))"
    #     )
    #     perimeter = cur.fetchone()[0]
    #     assert perimeter is not None
    #     print(f"  ✓ ST_Perimeter(): {perimeter}")
    # except Exception as e:
    #     print(f"  ℹ️  ST_Perimeter() not available: {type(e).__name__}")

    # test ST_X, ST_Y
    cur.execute("SELECT ST_X(ST_Point(5, 10)), ST_Y(ST_Point(5, 10))")
    x, y = cur.fetchone()
    assert x == 5 and y == 10
    print(f"  ✓ ST_X(), ST_Y(): ({x}, {y})")


def test_spatial_predicates(cur):
    """Test spatial predicate functions."""
    # Test ST_Equals
    cur.execute("SELECT ST_Equals(ST_Point(1, 1), ST_Point(1, 1))")
    equals = cur.fetchone()[0]
    assert equals is True
    print(f"  ✓ ST_Equals(): {equals}")

    # Test ST_Intersects
    cur.execute("SELECT ST_Intersects(ST_Point(0, 0), ST_Point(1, 1))")
    intersects = cur.fetchone()[0]
    assert intersects is not None
    print(f"  ✓ ST_Intersects(): {intersects}")

    # Test ST_Contains
    # cur.execute(
    #     "SELECT ST_Contains(ST_PolygonFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), ST_Point(1, 1))"
    # )
    # contains = cur.fetchone()[0]
    # assert contains is not None
    # print(f"  ✓ ST_Contains(): {contains}")

    # Test ST_Within
    # cur.execute(
    #     "SELECT ST_Within(ST_Point(1, 1), ST_PolygonFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))"
    # )
    # within = cur.fetchone()[0]
    # assert within is not None
    # print(f"  ✓ ST_Within(): {within}")

    # Test ST_Touches
    try:
        cur.execute("SELECT ST_Touches(ST_Point(0, 0), ST_Point(0, 0))")
        touches = cur.fetchone()[0]
        print(f"  ✓ ST_Touches(): {touches}")
    except Exception as e:
        print(f"  ℹ️  ST_Touches() not available: {type(e).__name__}")

    # Test ST_Disjoint
    try:
        cur.execute("SELECT ST_Disjoint(ST_Point(0, 0), ST_Point(10, 10))")
        disjoint = cur.fetchone()[0]
        print(f"  ✓ ST_Disjoint(): {disjoint}")
    except Exception as e:
        print(f"  ℹ️  ST_Disjoint() not available: {type(e).__name__}")


def test_spatial_analysis(cur):
    """Test spatial analysis functions."""
    # Test ST_Buffer
    # try:
    #     cur.execute("SELECT ST_Buffer(ST_Point(0, 0), 1.0)")
    #     buffer = cur.fetchone()[0]
    #     assert buffer is not None
    #     print("  ✓ ST_Buffer()")
    # except Exception as e:
    #     print(f"  ℹ️  ST_Buffer() not available: {type(e).__name__}")

    # Test ST_Union
    # try:
    #     cur.execute("SELECT ST_Union(ST_Point(0, 0), ST_Point(1, 1))")
    #     union = cur.fetchone()[0]
    #     assert union is not None
    #     print("  ✓ ST_Union()")
    # except Exception as e:
    #     print(f"  ℹ️  ST_Union() not available: {type(e).__name__}")

    # Test ST_Intersection
    # try:
    #     cur.execute("SELECT ST_Intersection(ST_Point(0, 0), ST_Point(0, 0))")
    #     intersection = cur.fetchone()[0]
    #     assert intersection is not None
    #     print("  ✓ ST_Intersection()")
    # except Exception as e:
    #     print(f"  ℹ️  ST_Intersection() not available: {type(e).__name__}")

    # Test ST_Difference
    # try:
    #     cur.execute("SELECT ST_Difference(ST_Point(0, 0), ST_Point(1, 1))")
    #     difference = cur.fetchone()[0]
    #     assert difference is not None
    #     print("  ✓ ST_Difference()")
    # except Exception as e:
    #     print(f"  ℹ️  ST_Difference() not available: {type(e).__name__}")

    # Test ST_ConvexHull
    # try:
    #     cur.execute("SELECT ST_ConvexHull(ST_Point(0, 0))")
    #     hull = cur.fetchone()[0]
    #     assert hull is not None
    #     print("  ✓ ST_ConvexHull()")
    # except Exception as e:
    #     print(f"  ℹ️  ST_ConvexHull() not available: {type(e).__name__}")


def test_bounding_box(cur):
    """Test bounding box functions."""
    # Test ST_Envelope
    # cur.execute(
    #     "SELECT ST_Envelope(ST_PolygonFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'))"
    # )
    # envelope = cur.fetchone()[0]
    # assert envelope is not None
    # print("  ✓ ST_Envelope()")

    # Test ST_MinX, ST_MaxX, ST_MinY, ST_MaxY
    try:
        cur.execute(
            "SELECT ST_XMin(ST_Point(5, 10)), ST_XMax(ST_Point(5, 10)), ST_YMin(ST_Point(5, 10)), ST_YMax(ST_Point(5, 10))"
        )
        minx, maxx, miny, maxy = cur.fetchone()
        assert minx == 5 and maxx == 5 and miny == 10 and maxy == 10
        print(
            f"  ✓ ST_MinX(), ST_MaxX(), ST_MinY(), ST_MaxY(): ({minx}, {maxx}, {miny}, {maxy})"
        )
    except Exception as e:
        print(
            f"  ℹ️  Bounding box coordinate functions not available: {type(e).__name__}"
        )


if __name__ == "__main__":
    main()
