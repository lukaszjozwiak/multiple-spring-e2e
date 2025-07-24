param(
    [Parameter(Mandatory = $true)]
    [string]$Path
)

# Escape every regex metacharacter in the path automatically
$rx = [regex]::Escape($Path)      # turns  e2e\src\Foo.java  →  e2e\\src\\Foo\.java

# Prepend .* so it matches no matter what absolute path Spotless uses internally
$rx = ".*$rx"

Write-Host "spotlessFiles = $rx"

# Run Maven
& mvn spotless:apply "-DspotlessFiles=$rx"

if ($LASTEXITCODE) {
    throw "Spotless failed (exit $LASTEXITCODE)"
}