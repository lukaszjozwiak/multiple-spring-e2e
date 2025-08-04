# Formatting config

Program: `powershell.exe`
Arguments: `-NoLogo -NoProfile -ExecutionPolicy Bypass -File "$ProjectFileDir$\spotless-onefile.ps1" "$FileRelativePath$"`
Dir: `$ProjectFileDir$`
Script:
```
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
```

# Checstyle config

```xml
<!-- build/corp-parent/pom.xml -->
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-checkstyle-plugin</artifactId>

  <!-- put the helper JAR on the plugin’s classpath -->
  <dependencies>
    <dependency>
      <groupId>com.mycorp.build</groupId>
      <artifactId>checkstyle-config</artifactId>
      <version>${project.version}</version>
    </dependency>
    <!-- your chosen Checkstyle engine -->
    <dependency>
      <groupId>com.puppycrawl.tools</groupId>
      <artifactId>checkstyle</artifactId>
      <version>10.26.1</version>
    </dependency>
  </dependencies>

  <executions>
    <execution>
      <phase>verify</phase>
      <goals><goal>check</goal></goals>
    </execution>
  </executions>

  <configuration>
    <!-- now resolved from the plugin class‑path -->
    <configLocation>custom-checkstyle-config.xml</configLocation>
    <consoleOutput>true</consoleOutput>
    <failOnViolation>true</failOnViolation>
  </configuration>
</plugin>
```
