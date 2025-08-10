package grok

import (
	"fmt"
	"sync"

	"log"

	"github.com/elastic/go-grok"
)

type GrokRepository interface {
	AddPattern(name string, pattern string) error
	GetPattern(name string) (string, error)
	ListPatterns() (map[string]string, error)
	RemovePattern(name string) error
	UpdatePattern(name string, pattern string) error
}

type dynamicGrok struct {
	repo              GrokRepository
	liveUpdateCh      chan interface{}
	mainPattern       string
	grok              *grok.Grok
	namedCapturesOnly bool
	mutex             sync.Mutex
}

func NewDynamicGrok(repo GrokRepository) *dynamicGrok {
	g := grok.New()
	dynamicGrok := &dynamicGrok{
		repo:         repo,
		liveUpdateCh: make(chan interface{}, 1), // Buffered channel for live updates
		grok:         g,
		mutex:        sync.Mutex{},
	}
	go dynamicGrok.initializeLiveUpdates()
	return dynamicGrok
}

func (dg *dynamicGrok) CompileByRepo(patternName string, namedCapturesOnly bool) error {
	pattern, err := dg.repo.GetPattern(patternName)
	if err != nil {
		return fmt.Errorf("failed to get pattern %s: %w", patternName, err)
	}
	dg.mainPattern = pattern
	dg.namedCapturesOnly = namedCapturesOnly
	if err := dg.grok.Compile(pattern, namedCapturesOnly); err != nil {
		return fmt.Errorf("failed to compile pattern %s: %w", patternName, err)
	}
	return nil
}

func (dg *dynamicGrok) CompileWithPattern(pattern string, namedCapturesOnly bool) error {
	dg.mainPattern = pattern
	dg.namedCapturesOnly = namedCapturesOnly
	if err := dg.grok.Compile(pattern, namedCapturesOnly); err != nil {
		return fmt.Errorf("failed to compile pattern %s: %w", pattern, err)
	}
	return nil
}

func (dg *dynamicGrok) AddPattern(name string, pattern string) error {
	if err := dg.repo.AddPattern(name, pattern); err != nil {
		return err
	}
	return nil
}

func (dg *dynamicGrok) GetPattern(name string) (string, error) {
	pattern, err := dg.repo.GetPattern(name)
	if err != nil {
		return "", err
	}
	return pattern, nil
}

func (dg *dynamicGrok) ListPatterns() (map[string]string, error) {
	patterns, err := dg.repo.ListPatterns()
	if err != nil {
		return nil, err
	}
	return patterns, nil
}

func (dg *dynamicGrok) RemovePattern(name string) error {
	if err := dg.repo.RemovePattern(name); err != nil {
		return err
	}
	dg.liveUpdateCh <- struct{}{}
	return nil
}

func (dg *dynamicGrok) UpdatePattern(name string, pattern string) error {
	if err := dg.repo.UpdatePattern(name, pattern); err != nil {
		return err
	}
	dg.liveUpdateCh <- struct{}{}
	return nil
}

func (dg *dynamicGrok) initializeLiveUpdates() {
	go func() {
		for range dg.liveUpdateCh {
			if dg.mainPattern == "" {
				continue
			}

			// Fetch updated patterns from the repository
			patterns, err := dg.repo.ListPatterns()
			if err != nil {
				log.Print("Error fetching patterns:", err)
				continue
			}
			dg.mutex.Lock()
			// Recompile Grok with updated patterns
			if err := dg.grok.AddPatterns(patterns); err != nil {
				log.Print("Error adding patterns:", err)
				continue
			}
			if err := dg.grok.Compile(dg.mainPattern, dg.namedCapturesOnly); err != nil {
				log.Print("Error compiling patterns:", err)
			}
			dg.mutex.Unlock()
		}
	}()
}
func (dg *dynamicGrok) HasCaptureGroups() bool {
	return dg.grok.HasCaptureGroups()
}

func (dg *dynamicGrok) Match(text []byte) bool {
	return dg.grok.Match(text)
}
func (dg *dynamicGrok) MatchString(text string) bool {
	return dg.grok.MatchString(text)
}

func (dg *dynamicGrok) Parse(text []byte) (map[string][]byte, error) {
	return dg.grok.Parse(text)
}

func (dg *dynamicGrok) ParseString(text string) (map[string]string, error) {
	return dg.grok.ParseString(text)
}

func (dg *dynamicGrok) ParseTyped(text []byte) (map[string]interface{}, error) {
	return dg.grok.ParseTyped(text)
}

func (dg *dynamicGrok) ParseTypedString(text string) (map[string]interface{}, error) {
	return dg.grok.ParseTypedString(text)
}
