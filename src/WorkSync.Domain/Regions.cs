using System.Collections.Frozen;

namespace WorkSync.Domain;

public sealed record RegionInfo(CloudRegion Region, Continent Continent, string TimeZoneId, TimeSpan FallbackUtcOffset);

/// <summary>Static catalogue of regions with their continent and local time zone (IANA id, fixed-offset fallback).</summary>
public static class Regions
{
    private static readonly FrozenDictionary<CloudRegion, RegionInfo> s_regions = new RegionInfo[]
    {
        new(CloudRegion.UsEast, Continent.NorthAmerica, "America/New_York", TimeSpan.FromHours(-5)),
        new(CloudRegion.UsWest, Continent.NorthAmerica, "America/Los_Angeles", TimeSpan.FromHours(-8)),
        new(CloudRegion.CanadaCentral, Continent.NorthAmerica, "America/Toronto", TimeSpan.FromHours(-5)),
        new(CloudRegion.BrazilSouth, Continent.SouthAmerica, "America/Sao_Paulo", TimeSpan.FromHours(-3)),
        new(CloudRegion.EuWest, Continent.Europe, "Europe/Amsterdam", TimeSpan.FromHours(1)),
        new(CloudRegion.EuNorth, Continent.Europe, "Europe/Stockholm", TimeSpan.FromHours(1)),
        new(CloudRegion.UkSouth, Continent.Europe, "Europe/London", TimeSpan.Zero),
        new(CloudRegion.AsiaEast, Continent.Asia, "Asia/Hong_Kong", TimeSpan.FromHours(8)),
        new(CloudRegion.AsiaSouth, Continent.Asia, "Asia/Kolkata", TimeSpan.FromHours(5.5)),
        new(CloudRegion.JapanEast, Continent.Asia, "Asia/Tokyo", TimeSpan.FromHours(9)),
        new(CloudRegion.AustraliaEast, Continent.Oceania, "Australia/Sydney", TimeSpan.FromHours(10)),
    }.ToFrozenDictionary(r => r.Region);

    private static readonly FrozenDictionary<CloudRegion, TimeZoneInfo> s_timeZones =
        s_regions.Values.ToFrozenDictionary(r => r.Region, ResolveTimeZone);

    public static IReadOnlyCollection<CloudRegion> All => s_regions.Keys;

    public static RegionInfo Get(CloudRegion region) => s_regions[region];

    public static DateTimeOffset ToLocalTime(CloudRegion region, DateTimeOffset utc) =>
        TimeZoneInfo.ConvertTime(utc, s_timeZones[region]);

    public static RegionDistance DistanceBetween(CloudRegion a, CloudRegion b) =>
        a == b ? RegionDistance.SameRegion
        : Get(a).Continent == Get(b).Continent ? RegionDistance.SameContinent
        : RegionDistance.CrossContinent;

    private static TimeZoneInfo ResolveTimeZone(RegionInfo info)
    {
        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById(info.TimeZoneId);
        }
        catch (Exception ex) when (ex is TimeZoneNotFoundException or InvalidTimeZoneException)
        {
            return TimeZoneInfo.CreateCustomTimeZone(info.TimeZoneId, info.FallbackUtcOffset, info.TimeZoneId, info.TimeZoneId);
        }
    }
}
