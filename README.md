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

# Fixing kafka streams leftovers

```java
Path tmp = Files.createTempDirectory("kstreams-" + UUID.randomUUID());
String appId = "my-app-" + UUID.randomUUID();

List<String> cmd = List.of(
    "java",
    "-Dspring.profiles.active=test",
    "-Dspring.kafka.streams.application-id=" + appId,
    "-Dspring.kafka.streams.state-dir=" + tmp.toAbsolutePath(),
    "-Dspring.kafka.streams.cleanup.on-startup=true",
    "-Dspring.kafka.streams.cleanup.on-shutdown=true",
    "-jar", "my-spring-kstreams-app.jar"
);

Process p = new ProcessBuilder(cmd).inheritIO().start();

p.destroyForcibly(); // if needed
Files.walk(tmp)
     .sorted(Comparator.reverseOrder())
    .forEach(path -> { try { Files.deleteIfExists(path); } catch (IOException ignored) {} })
```

# OC commands
```bash
oc get pods -o=jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.containerStatuses[0].image}{"\t"}{.status.startTime}{"\n"}{end}'
oc get pods --field-selector=status.phase=Running -o=jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.containerStatuses[0].image}{"\t"}{.status.startTime}{"\n"}{end}' | grep -vE 'deploy$'

```
