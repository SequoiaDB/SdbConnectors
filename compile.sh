#!/bin/bash

set -o errexit  # Exit the script with error if any of the commands fail

ROOT_PATH="$(pwd)"

GIT_VERSION="$(git describe --abbrev=7 --tags)"
VERSION="${GIT_VERSION:1}"
COMPILE_TYPE="all"
SPECIFY_JAVA_DRIVER=""
RELEASE=0

FLINK_PATH="$ROOT_PATH/flink"
SPARK2_PATH="$ROOT_PATH/spark"
SPARK3_PATH="$ROOT_PATH/spark-3.0"

OUTPUT_PATH="$ROOT_PATH/build"
FLINK_TARGET_PATH="$FLINK_PATH/target"
SPARK2_TARGET_PATH="$SPARK2_PATH/target"
SPARK3_TARGET_PATH="$SPARK3_PATH/target"

FLINK_JAR_PREFIX="sdb-flink-connector-"
SPARK_JAR_PREFIX="spark-sequoiadb"

clean_output_path() {
    if [ -d "$OUTPUT_PATH" ]; then
        rm -r "$OUTPUT_PATH"
        echo "clean '$directory'"
    fi

    mkdir -p "$OUTPUT_PATH"
    echo "create '$OUTPUT_PATH'"
}

desc() {
	set +o xtrace
	local msg="$@"
	printf "\n-----------------------------------------------------------------------------------\n"
	printf "$msg"
    printf "\n-----------------------------------------------------------------------------------\n"
}

compile_flink() {
    desc "compile flink"
    cd $FLINK_PATH
    mvn versions:set -DnewVersion=$VERSION
    mvn clean package -Dmaven.test.skip=true $SPECIFY_JAVA_DRIVER
}

compile_spark2() {
    desc "compile spark2"
    cd $SPARK2_PATH
    mvn versions:set -DnewVersion=$VERSION
    mvn clean package -Dmaven.test.skip=true $SPECIFY_JAVA_DRIVER
}

compile_spark3() {
    desc "compile spark3"
    cd $SPARK3_PATH
    mvn versions:set -DnewVersion=$VERSION
    mvn clean package -Dmaven.test.skip=true $SPECIFY_JAVA_DRIVER
}

compile_all() {
    compile_flink
    compile_spark2
    compile_spark3
}

copy_file() {
    local source_dir="$1"
    local prefix="$2"
    local suffix1="-sources.jar"
    local suffix2="-javadoc.jar"

    # copy file
    for file in "$source_dir"/*; do
        if [ ! -f "$file" ]; then
            continue
        fi

        file_name=$(basename "$file")
        if [[ $file_name != "$prefix"* ]]; then
            continue
        fi

        # skip -sources.jar and -javadoc.jar
        if [[ $file_name == *"$suffix1" ]]; then
            continue
        fi
        if [[ $file_name == *"$suffix2" ]]; then
            continue
        fi

        cp "$file" "$OUTPUT_PATH"
        echo "success: copy '$file' to '$OUTPUT_PATH'"
    done
}

pack_jars() {
    cd $ROOT_PATH

    if [ "$COMPILE_TYPE" == "flink" ] || [ "$COMPILE_TYPE" == "all" ]; then
        copy_file $FLINK_TARGET_PATH $FLINK_JAR_PREFIX
    fi

    if [ "$COMPILE_TYPE" == "spark2" ] || [ "$COMPILE_TYPE" == "all" ]; then
        copy_file $SPARK2_TARGET_PATH $SPARK_JAR_PREFIX
    fi

    if [ "$COMPILE_TYPE" == "spark3" ] || [ "$COMPILE_TYPE" == "all" ]; then
        copy_file $SPARK3_TARGET_PATH $SPARK_JAR_PREFIX
    fi

    cd $OUTPUT_PATH

    if [ "$COMPILE_TYPE" == "flink" ] || [ "$COMPILE_TYPE" == "all" ]; then
        local flinkPackName=$FLINK_JAR_PREFIX""$VERSION".tar.gz"
        eval tar -zcf $flinkPackName "$FLINK_JAR_PREFIX*"
    fi

    if [ "$COMPILE_TYPE" == "spark2" ] || [ "$COMPILE_TYPE" == "spark3" ] || [ "$COMPILE_TYPE" == "all" ]; then
        local sparkPackName=$SPARK_JAR_PREFIX"-"$VERSION".tar.gz"
        eval tar -zcf $sparkPackName "$SPARK_JAR_PREFIX*"
    fi
}

buildHelp()
{
    echo "Usage:"
    echo "  compile.sh --type all"
    echo ""
    echo "Options:"
    echo "  -h, help:          dispaly help information"
    echo "  type:          compile type, support flink | spark2 | spark3 | all, default is all"
    echo "  driver:        sequoiadb java driver version, default to the version used in the pom file"
    echo "  release:       compile release, default is snapshot"
    exit 0
}

############################################
#            Main Program                  #
############################################

ARGS=`getopt -o ht:d:r --long help,release,type:,driver: -n 'compile.sh' -- "$@"`
ret=$?
test $ret -ne 0 && return $ret
eval set -- "${ARGS}"

while true
do
   case "$1" in
      -t |--type )      COMPILE_TYPE="$2"
                        shift 2
                        ;;
      -d |--driver )    SPECIFY_JAVA_DRIVER="-Dsequoiadb.driver.version=$2"
                        shift 2
                        ;;
      -h | --help )     buildHelp
                        shift 1
                        break
                        ;;
      -r | --release )  RELEASE=1
                        shift 1
                        break
                        ;;
      --)               shift
                        break
                        ;;
      * )               echo "Invalid parameters: $1"
                        exit 1
                        ;;
   esac
done

if [ $RELEASE -ne 1 ]; then
    VERSION="${VERSION}-SNAPSHOT"
fi

desc "Compiling SequoiaDB Connector"

# 1. env version
desc "Java version:"
java -version

desc "Maven version:"
mvn -version

# 2. clean
clean_output_path

# 3. compile jar
if [ "$COMPILE_TYPE" == "flink" ]; then
    compile_flink
elif [ "$COMPILE_TYPE" == "spark2" ]; then
    compile_flink
elif [ "$COMPILE_TYPE" == "spark3" ]; then
    compile_flink
elif [ "$COMPILE_TYPE" == "all" ]; then
    compile_all
else
    echo "Invalid compile type: $COMPILE_TYPE"
    exit 1
fi

# 4. copy and pack
desc "pack jars"
pack_jars