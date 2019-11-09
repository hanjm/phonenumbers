package phonenumbers

import (
	"bufio"
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"bytes"

	"github.com/golang/protobuf/proto"
)

// Update will update all metadata and then take effect atomic
func Update() error {
	if err := updateMetadataAndAtomicReplaceVar(); err != nil {
		return fmt.Errorf("failed to update metadata:%s", err)
	}
	if err := loadDataAndAtomicReplaceVar(); err != nil {
		return fmt.Errorf("failed to load metadata:%s", err)
	}
	return nil
}

func updateMetadataAndAtomicReplaceVar() (err error) {
	metadata, metadataData, err := buildMetadata()
	if err != nil {
		return err
	}
	regionMapData, err := buildRegions(metadata)
	if err != nil {
		return err
	}
	timezoneMapData, err := buildTimezones()
	if err != nil {
		return err
	}
	carrierMapData, err := buildPrefixData(&carrier)
	if err != nil {
		return err
	}
	geocodingMapData, err := buildPrefixData(&geocoding)
	if err != nil {
		return err
	}
	// atomic store
	_metadataData.Store(metadataData)
	_regionMapData.Store(regionMapData)
	_timezoneMapData.Store(timezoneMapData)
	_carrierMapData.Store(carrierMapData)
	_geocodingMapData.Store(geocodingMapData)
	return nil
}

type prefixBuild struct {
	url     string
	dir     string
	srcPath string
	varName string
}

const (
	metadataURL = "https://raw.githubusercontent.com/googlei18n/libphonenumber/master/resources/PhoneNumberMetadata.xml"
	tzURL       = "https://raw.githubusercontent.com/googlei18n/libphonenumber/master/resources/timezones/map_data.txt"
)

var carrier = prefixBuild{
	url: "https://github.com/googlei18n/libphonenumber/trunk/resources/carrier",
	dir: "carrier",
}

var geocoding = prefixBuild{
	url: "https://github.com/googlei18n/libphonenumber/trunk/resources/geocoding",
	dir: "geocoding",
}

func buildMetadata() (*PhoneMetadataCollection, string, error) {
	log.Println("Fetching PhoneNumberMetadata.xml from Github")
	body, err := fetchURL(metadataURL)
	if err != nil {
		return nil, "", err
	}

	log.Println("Building new metadata collection")
	collection, err := BuildPhoneMetadataCollection(body, false, false)
	if err != nil {
		err = fmt.Errorf("error converting XML: %s", err)
		return nil, "", err
	}

	// write it out as a protobuf
	data, err := proto.Marshal(collection)
	if err != nil {
		err = fmt.Errorf("rrror marshalling metadata: %v", err)
	}
	return collection, gzipBytesAndBase64(data), nil
}

func fetchURL(url string) ([]byte, error) {
	resp, err := (&http.Client{
		Timeout: time.Minute,
	}).Get(url)
	if err != nil || resp.StatusCode != 200 {
		err = fmt.Errorf("error fetching URL '%s': %v", url, err)
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("error reading body: %s", err)
		return nil, err
	}
	return body, nil
}

func svnExport(dir string, url string) (err error) {
	_ = os.RemoveAll(dir)
	cmd := exec.Command(
		"/bin/bash",
		"-c",
		fmt.Sprintf("svn export %s --force", url),
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err = cmd.Start(); err != nil {
		return err
	}
	_, err = ioutil.ReadAll(stderr)
	if err != nil {
		return err
	}
	outputBuf := bufio.NewReader(stdout)

	for {
		output, _, err := outputBuf.ReadLine()
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		log.Println(string(output))
	}

	if err = cmd.Wait(); err != nil {
		return err
	}
	return nil
}

func buildRegions(metadata *PhoneMetadataCollection) (string, error) {
	regionMap := BuildCountryCodeToRegionMap(metadata)
	result, err := intStringArrayMapToString(regionMap)
	if err != nil {
		return "", err
	}
	return result, nil
}

func buildTimezones() (string, error) {
	log.Println("Building timezone map")
	body, err := fetchURL(tzURL)
	if err != nil {
		return "", err
	}

	// build our map of prefix to timezones
	prefixMap := make(map[int][]string)
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}

		if strings.TrimSpace(line) == "" {
			continue
		}

		fields := strings.Split(line, "|")
		if len(fields) != 2 {
			err = fmt.Errorf("invalid format in timezone file: %s", line)
			return "", err
		}

		zones := strings.Split(fields[1], "&")
		if len(zones) < 1 {
			err = fmt.Errorf("invalid format in timezone file: %s", line)
			return "", err
		}

		// parse our prefix
		prefix, err := strconv.Atoi(fields[0])
		if err != nil {
			err = fmt.Errorf("invalid prefix in line: %s", line)
			return "", err
		}
		prefixMap[prefix] = zones
	}
	result, err := intStringArrayMapToString(prefixMap)
	if err != nil {
		return "", nil
	}
	return result, nil
}

func intStringArrayMapToString(prefixMap map[int][]string) (dst string, err error) {
	// build lists of our keys and values
	keys := make([]int, 0, len(prefixMap))
	values := make([]string, 0, 255)
	seenValues := make(map[string]bool, 255)

	for k, vs := range prefixMap {
		keys = append(keys, k)
		for _, v := range vs {
			_, seen := seenValues[v]
			if !seen {
				seenValues[v] = true
				values = append(values, v)
			}
		}
	}
	sort.Strings(values)
	sort.Ints(keys)

	internMap := make(map[string]int, len(values))
	for i, v := range values {
		internMap[v] = i
	}

	data := &bytes.Buffer{}

	// first write our values, as length of string and raw bytes
	joinedValues := strings.Join(values, "\n")
	if err := binary.Write(data, binary.LittleEndian, uint32(len(joinedValues))); err != nil {
		return "", err
	}
	if err := binary.Write(data, binary.LittleEndian, []byte(joinedValues)); err != nil {
		return "", err
	}

	// then the number of keys
	if err := binary.Write(data, binary.LittleEndian, uint32(len(keys))); err != nil {
		return "", err
	}

	// we write our key / value pairs as a varint of the difference of the previous prefix
	// and a uint16 of the value index
	last := 0
	intBuf := make([]byte, 6)
	for _, key := range keys {
		// first write our prefix
		diff := key - last
		l := binary.PutUvarint(intBuf, uint64(diff))
		if err := binary.Write(data, binary.LittleEndian, intBuf[:l]); err != nil {
			return "", err
		}

		// then our values
		values := prefixMap[key]

		// write our number of values
		if err := binary.Write(data, binary.LittleEndian, uint8(len(values))); err != nil {
			return "", err
		}

		// then each value as the interned index
		for _, v := range values {
			valueIntern := internMap[v]
			if err := binary.Write(data, binary.LittleEndian, uint16(valueIntern)); err != nil {
				return "", err
			}
		}

		last = key
	}
	return gzipBytesAndBase64(data.Bytes()), nil
}

func gzipBytesAndBase64(data []byte) string {
	var compressed bytes.Buffer
	w := gzip.NewWriter(&compressed)
	_, _ = w.Write(data)
	_ = w.Close()
	encoded := base64.StdEncoding.EncodeToString(compressed.Bytes())
	return encoded
}

func buildPrefixData(build *prefixBuild) (map[string]string, error) {
	prefixDataMap := make(map[string]string)
	log.Println("Fetching " + build.url + " from Github")
	err := svnExport(build.dir, build.url)
	if err != nil {
		return nil, err
	}
	// get our top level language directories
	dirs, err := filepath.Glob(fmt.Sprintf("%s/*", build.dir))
	if err != nil {
		return nil, err
	}

	// for each directory
	languageMappings := make(map[string]map[int]string)
	for _, dir := range dirs {
		// only look at directories
		fi, _ := os.Stat(dir)
		if !fi.IsDir() {
			log.Printf("Ignoring directory: %s\n", dir)
			continue
		}

		// get our language code
		parts := strings.Split(dir, "/")

		// build a map for that directory
		mappings, err := readMappingsForDir(dir)
		if err != nil {
			return nil, err
		}

		// save it for our language
		languageMappings[parts[1]] = mappings
	}

	for lang, mappings := range languageMappings {
		// iterate through our map, creating our full set of values and prefixes
		prefixes := make([]int, 0, len(mappings))
		seenValues := make(map[string]bool)
		values := make([]string, 0, 255)
		for prefix, value := range mappings {
			prefixes = append(prefixes, prefix)
			_, seen := seenValues[value]
			if !seen {
				values = append(values, value)
				seenValues[value] = true
			}
		}

		// make sure we won't overrun uint16s
		if len(values) > math.MaxUint16 {
			return nil, fmt.Errorf("too many values to represent in uint16")
		}

		// need sorted prefixes for our diff writing to work
		sort.Ints(prefixes)

		// sorted values compress better
		sort.Strings(values)

		// build our reverse mapping from value to offset
		internMappings := make(map[string]uint16)
		for i, value := range values {
			internMappings[value] = uint16(i)
		}

		// write our map
		data := &bytes.Buffer{}

		// first write our values, as length of string and raw bytes
		joinedValues := strings.Join(values, "\n")
		if err = binary.Write(data, binary.LittleEndian, uint32(len(joinedValues))); err != nil {
			return nil, err
		}
		if err = binary.Write(data, binary.LittleEndian, []byte(joinedValues)); err != nil {
			return nil, err
		}

		// then then number of prefix / value pairs
		if err = binary.Write(data, binary.LittleEndian, uint32(len(prefixes))); err != nil {
			return nil, err
		}

		// we write our prefix / value pairs as a varint of the difference of the previous prefix
		// and a uint16 of the value index
		last := 0
		intBuf := make([]byte, 6)
		for _, prefix := range prefixes {
			value := mappings[prefix]
			valueIntern := internMappings[value]
			diff := prefix - last
			l := binary.PutUvarint(intBuf, uint64(diff))
			if err = binary.Write(data, binary.LittleEndian, intBuf[:l]); err != nil {
				return nil, err
			}
			if err = binary.Write(data, binary.LittleEndian, uint16(valueIntern)); err != nil {
				return nil, err
			}

			last = prefix
		}

		var compressed bytes.Buffer
		w := gzip.NewWriter(&compressed)
		_, _ = w.Write(data.Bytes())
		_ = w.Close()
		c := base64.StdEncoding.EncodeToString(compressed.Bytes())
		prefixDataMap[lang] = c
	}

	return prefixDataMap, nil
}

func readMappingsForDir(dir string) (map[int]string, error) {
	log.Printf("Building map for: %s\n", dir)
	mappings := make(map[int]string)

	files, err := filepath.Glob(dir + "/*.txt")
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		body, err := ioutil.ReadFile(file)
		if err != nil {
			return nil, err
		}
		items := strings.Split(file, "/")
		if len(items) != 3 {
			err = fmt.Errorf("file name %s not correct", file)
		}

		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, "#") {
				continue
			}
			if strings.TrimSpace(line) == "" {
				continue
			}
			fields := strings.Split(line, "|")
			if len(fields) != 2 {
				continue
			}
			prefix := fields[0]
			prefixInt, err := strconv.Atoi(prefix)
			if err != nil || prefixInt < 0 {
				err = fmt.Errorf("unable to parse line: %s", line)
				return nil, err
			}

			value := strings.TrimSpace(fields[1])
			if value == "" {
				log.Printf("Ignoring empty value: %s", line)
			}

			_, repeat := mappings[prefixInt]
			if repeat {
				err = fmt.Errorf("repeated prefix for line: %s", line)
				return nil, err
			}
			mappings[prefixInt] = fields[1]
		}
	}

	log.Printf("Read %d mappings in %s\n", len(mappings), dir)
	return mappings, nil
}
