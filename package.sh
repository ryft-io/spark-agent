echo "Mvn package"

echo "Cleaning up target directory before packaging"

REVISION=$1

if [ -z "$REVISION" ]; then
  echo "Error: Revision is required"
  exit 1
fi

mvn clean

mvn package -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Drevision=$REVISION

# Extract the version
mvn_version=$(mvn -q -Drevision=$REVISION -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)

# Extract the artifactId
artifact_id=$(mvn -q -Drevision=$REVISION -Dexec.executable=echo -Dexec.args='${project.artifactId}' --non-recursive exec:exec)

# Form the JAR name using artifactId and version
JAR_NAME="${artifact_id}-$mvn_version.jar"

# Define S3 destination
s3_destination="s3://ryft-spark-application-jars/ryft-spark-events-log-plugin/ryft-spark-events-log-writer-plugin-$mvn_version.jar"
echo "Copying jar: ${JAR_NAME} to ${s3_destination}"

# Copy the JAR to S3
aws s3 cp "target/$JAR_NAME" "$s3_destination" --profile demo

# Install the package locally:
# shellcheck disable=SC2046
mvn install:install-file -Dfile=target/"$JAR_NAME" -DgroupId=io.ryft -DartifactId=spark -Dversion="$mvn_version" -Dpackaging=jar
