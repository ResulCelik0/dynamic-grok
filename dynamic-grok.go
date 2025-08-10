package dynamicgrok

import (
	"fmt"

	"github.com/elastic/go-grok"
)

type GrokRepository interface {
	AddPattern(name string, pattern string) error
	GetPattern(name string) (string, error)
	ListPatterns() (map[string]string, error)
	RemovePattern(name string) error
	UpdatePattern(name string, pattern string) error
}

type DynamicGrok struct {
	repo         GrokRepository
	liveUpdateCh chan interface{}
	grok         *grok.Grok
}

func NewDynamicGrok(repo GrokRepository) *DynamicGrok {
	g := grok.New()
	dynamicGrok := &DynamicGrok{
		repo:         repo,
		liveUpdateCh: make(chan interface{}, 100), // Buffered channel for live updates
		grok:         g,
	}
	dynamicGrok.initializeLiveUpdates()
	return dynamicGrok
}

func (dg *DynamicGrok) AddPattern(name string, pattern string) error {
	if err := dg.repo.AddPattern(name, pattern); err != nil {
		return err
	}
	return nil
}

func (dg *DynamicGrok) GetPattern(name string) (string, error) {
	pattern, err := dg.repo.GetPattern(name)
	if err != nil {
		return "", err
	}
	return pattern, nil
}

func (dg *DynamicGrok) ListPatterns() (map[string]string, error) {
	patterns, err := dg.repo.ListPatterns()
	if err != nil {
		return nil, err
	}
	return patterns, nil
}

func (dg *DynamicGrok) RemovePattern(name string) error {
	if err := dg.repo.RemovePattern(name); err != nil {
		return err
	}
	return nil
}

func (dg *DynamicGrok) UpdatePattern(name string, pattern string) error {
	if err := dg.repo.UpdatePattern(name, pattern); err != nil {
		return err
	}
	dg.liveUpdateCh <- struct{}{} // Trigger live update
	return nil
}

func (dg *DynamicGrok) initializeLiveUpdates(pattern string) {
	go func() {
		for range dg.liveUpdateCh {
			// Fetch updated patterns from the repository
			patterns, err := dg.repo.ListPatterns()
			if err != nil {
				fmt.Println("Error fetching patterns:", err)
				continue
			}
			// Recompile Grok with updated patterns
			if err := dg.grok.AddPatterns(patterns); err != nil {
				fmt.Println("Error adding patterns:", err)
				continue
			}
		}
	}()
}
